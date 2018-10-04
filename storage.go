package tsrsmgo

import (
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/lab259/go-timeseries"
	"github.com/lab259/repository"
	"github.com/romana/rlog"
)

func NewErrOperationNotSupported(operation timesrs.OperationType) error {
	return fmt.Errorf("operation '%d' not supported", operation)
}

type Storage struct {
	Runner repository.QueryRunner
}

func (storage *Storage) Store(entry *timesrs.StorageEntry) error {
	err := storage.Runner.RunWithDB(func(db *mgo.Database) error {
		evtTime := entry.Event.Time()
		for _, granularity := range entry.Granularities {
			c := db.C(fmt.Sprintf("%s_%s", entry.Collection, granularity.Collection.String()))
			data := make(map[string]interface{})
			dataInc := make(map[string]interface{})
			dataSet := make(map[string]interface{})
			for _, v := range entry.Result.Data {
				d := granularity.Record.RelativeTo(granularity.Collection.Truncate(evtTime), evtTime)
				switch v.Type {
				case timesrs.OperationTypeInc:
					dataInc[v.Field] = v.Value
					dataInc[fmt.Sprintf("records.%d.%s", d, v.Field)] = v.Value
				case timesrs.OperationTypeSet:
					dataSet[fmt.Sprintf("%s.value", v.Field)] = v.Value               // Last value
					dataInc[fmt.Sprintf("%s.total", v.Field)] = v.Value               // Total sum
					dataInc[fmt.Sprintf("%s.count", v.Field)] = 1                     // Count
					dataSet[fmt.Sprintf("records.%d.%s.value", d, v.Field)] = v.Value // Last value
					dataInc[fmt.Sprintf("records.%d.%s.total", d, v.Field)] = v.Value // Count
					dataInc[fmt.Sprintf("records.%d.%s.count", d, v.Field)] = 1       // Total sum
				default:
					return NewErrOperationNotSupported(v.Type)
				}
			}
			if len(dataInc) > 0 {
				data["$inc"] = dataInc
			}
			if len(dataSet) > 0 {
				data["$set"] = dataSet
			}
			keys := make(map[string]interface{}, len(entry.Keys)+1)
			if len(entry.Keys) != 0 {
				for k, v := range entry.Keys {
					keys[k] = v
				}
				keys["at"] = granularity.Collection.Truncate(evtTime)
			} else {
				keys["_id"] = granularity.Collection.Truncate(evtTime)
			}
			ci, err := c.Upsert(keys, data)
			if err != nil {
				return err
			}
			rlog.Tracef(3, "inserted %d, updated %d", ci.Matched, ci.Updated)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
