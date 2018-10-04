package tsrsmgo_test

import (
	"github.com/globalsign/mgo"
	"github.com/lab259/go-timeseries"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"

	"."
)

type MgoRunner struct {
	session *mgo.Session
}

func (runner *MgoRunner) RunWithDB(fnc func(db *mgo.Database) error) error {
	return fnc(runner.session.DB(""))
}

var DefaultMgoRunner MgoRunner

func InitMgoRunner() {
	BeforeEach(func() {
		sess, err := mgo.DialWithInfo(&mgo.DialInfo{
			Addrs:    []string{"localhost"},
			Username: "",
			Database: "test",
			Password: "",
		})
		Expect(err).To(BeNil())
		DefaultMgoRunner.session = sess
		Expect(sess.DB("").DropDatabase()).To(BeNil())
	})
}

var _ = Describe("Time Series", func() {
	Describe("Mongo", func() {
		Describe("Storage", func() {
			InitMgoRunner()

			When("records are seconds", func() {
				It("should store the $inc data", func() {
					storage := tsrsmgo.Storage{
						Runner: &DefaultMgoRunner,
					}
					evtTime := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
					err := storage.Store(&timesrs.StorageEntry{
						Event:      timesrs.NewEvent("event", nil, evtTime),
						Collection: "aggregation1",
						Result: &timesrs.PipelineResult{
							Data: timesrs.AggregationData{
								*timesrs.NewOperationInc("field1", 1),
								*timesrs.NewOperationInc("field2", 2),
							},
						},
						Granularities: []timesrs.GranularityComposite{
							{
								Collection: timesrs.GranularityDay,
								Record:     timesrs.GranularitySecond,
							},
						},
					})
					Expect(err).To(BeNil())
					type aggregationStruct struct {
						Field1 int `bson:"field1"`
						Field2 int `bson:"field2"`
					}
					type aggregationStructDay struct {
						aggregationStruct `bson:",inline"`
						ID                time.Time                    `bson:"_id"`
						Records           map[string]aggregationStruct `bson:"records"`
					}
					Expect(DefaultMgoRunner.RunWithDB(func(db *mgo.Database) error {
						c := db.C("aggregation1_day")
						data := make([]aggregationStructDay, 0)
						Expect(c.Find(nil).All(&data)).To(BeNil())
						Expect(data).To(HaveLen(1))
						Expect(data[0].Field1).To(Equal(1))
						Expect(data[0].Field2).To(Equal(2))
						Expect(data[0].Records).To(HaveLen(1))
						Expect(data[0].Records).To(HaveKey("42424"))
						Expect(data[0].Records["42424"].Field1).To(Equal(1))
						Expect(data[0].Records["42424"].Field2).To(Equal(2))
						return nil
					})).To(BeNil())
				})

				It("should store the $inc data multiple times at same second", func() {
					storage := tsrsmgo.Storage{
						Runner: &DefaultMgoRunner,
					}
					evtTime := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
					err := storage.Store(&timesrs.StorageEntry{
						Event:      timesrs.NewEvent("event", nil, evtTime),
						Collection: "aggregation1",
						Result: &timesrs.PipelineResult{
							Data: timesrs.AggregationData{
								*timesrs.NewOperationInc("field1", 1),
								*timesrs.NewOperationInc("field2", 2),
							},
						},
						Granularities: []timesrs.GranularityComposite{
							{
								Collection: timesrs.GranularityDay,
								Record:     timesrs.GranularitySecond,
							},
						},
					})

					err = storage.Store(&timesrs.StorageEntry{
						Event:      timesrs.NewEvent("event", nil, evtTime),
						Collection: "aggregation1",
						Result: &timesrs.PipelineResult{
							Data: timesrs.AggregationData{
								*timesrs.NewOperationInc("field1", 3),
								*timesrs.NewOperationInc("field2", 1),
							},
						},
						Granularities: []timesrs.GranularityComposite{
							{
								Collection: timesrs.GranularityDay,
								Record:     timesrs.GranularitySecond,
							},
						},
					})
					Expect(err).To(BeNil())
					type aggregationStruct struct {
						Field1 int `bson:"field1"`
						Field2 int `bson:"field2"`
					}
					type aggregationStructDay struct {
						aggregationStruct `bson:",inline"`
						Records           map[string]aggregationStruct `bson:"records"`
					}
					Expect(DefaultMgoRunner.RunWithDB(func(db *mgo.Database) error {
						c := db.C("aggregation1_day")
						data := make([]aggregationStructDay, 0)
						Expect(c.Find(nil).All(&data)).To(BeNil())
						Expect(data).To(HaveLen(1))
						Expect(data[0].Field1).To(Equal(4))
						Expect(data[0].Field2).To(Equal(3))
						Expect(data[0].Records).To(HaveLen(1))
						Expect(data[0].Records).To(HaveKey("42424"))
						Expect(data[0].Records["42424"].Field1).To(Equal(4))
						Expect(data[0].Records["42424"].Field2).To(Equal(3))
						return nil
					})).To(BeNil())
				})

				It("should store the $inc data multiple times", func() {
					storage := tsrsmgo.Storage{
						Runner: &DefaultMgoRunner,
					}
					evtTime1 := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
					err := storage.Store(&timesrs.StorageEntry{
						Event:      timesrs.NewEvent("event", nil, evtTime1),
						Collection: "aggregation1",
						Result: &timesrs.PipelineResult{
							Data: timesrs.AggregationData{
								*timesrs.NewOperationInc("field1", 1),
								*timesrs.NewOperationInc("field2", 2),
							},
						},
						Granularities: []timesrs.GranularityComposite{
							{
								Collection: timesrs.GranularityDay,
								Record:     timesrs.GranularitySecond,
							},
						},
					})

					evtTime2 := time.Date(2018, time.October, 2, 11, 50, 44, 0, time.UTC)
					err = storage.Store(&timesrs.StorageEntry{
						Event:      timesrs.NewEvent("event", nil, evtTime2),
						Collection: "aggregation1",
						Result: &timesrs.PipelineResult{
							Data: timesrs.AggregationData{
								*timesrs.NewOperationInc("field1", 3),
								*timesrs.NewOperationInc("field2", 1),
							},
						},
						Granularities: []timesrs.GranularityComposite{
							{
								Collection: timesrs.GranularityDay,
								Record:     timesrs.GranularitySecond,
							},
						},
					})
					Expect(err).To(BeNil())
					type aggregationStruct struct {
						Field1 int `bson:"field1"`
						Field2 int `bson:"field2"`
					}
					type aggregationStructDay struct {
						aggregationStruct `bson:",inline"`
						Records           map[string]aggregationStruct `bson:"records"`
					}
					Expect(DefaultMgoRunner.RunWithDB(func(db *mgo.Database) error {
						c := db.C("aggregation1_day")
						data := make([]aggregationStructDay, 0)
						Expect(c.Find(nil).All(&data)).To(BeNil())
						Expect(data).To(HaveLen(1))
						Expect(data[0].Field1).To(Equal(4))
						Expect(data[0].Field2).To(Equal(3))
						Expect(data[0].Records).To(HaveLen(2))
						Expect(data[0].Records).To(HaveKey("42424"))
						Expect(data[0].Records).To(HaveKey("42644"))
						Expect(data[0].Records["42424"].Field1).To(Equal(1))
						Expect(data[0].Records["42424"].Field2).To(Equal(2))
						Expect(data[0].Records["42644"].Field1).To(Equal(3))
						Expect(data[0].Records["42644"].Field2).To(Equal(1))
						return nil
					})).To(BeNil())
				})
			})

			When("records are minutes", func() {
				It("should store the $inc data", func() {
					storage := tsrsmgo.Storage{
						Runner: &DefaultMgoRunner,
					}
					evtTime := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
					err := storage.Store(&timesrs.StorageEntry{
						Event:      timesrs.NewEvent("event", nil, evtTime),
						Collection: "aggregation1",
						Result: &timesrs.PipelineResult{
							Data: timesrs.AggregationData{
								*timesrs.NewOperationInc("field1", 1),
								*timesrs.NewOperationInc("field2", 2),
							},
						},
						Granularities: []timesrs.GranularityComposite{
							{
								Collection: timesrs.GranularityDay,
								Record:     timesrs.GranularityMinute,
							},
						},
					})
					Expect(err).To(BeNil())
					type aggregationStruct struct {
						Field1 int `bson:"field1"`
						Field2 int `bson:"field2"`
					}
					type aggregationStructDay struct {
						aggregationStruct `bson:",inline"`
						ID                time.Time                    `bson:"_id"`
						Records           map[string]aggregationStruct `bson:"records"`
					}
					Expect(DefaultMgoRunner.RunWithDB(func(db *mgo.Database) error {
						c := db.C("aggregation1_day")
						data := make([]aggregationStructDay, 0)
						Expect(c.Find(nil).All(&data)).To(BeNil())
						Expect(data).To(HaveLen(1))
						Expect(data[0].Field1).To(Equal(1))
						Expect(data[0].Field2).To(Equal(2))
						Expect(data[0].Records).To(HaveLen(1))
						Expect(data[0].Records["707"].Field1).To(Equal(1))
						Expect(data[0].Records["707"].Field2).To(Equal(2))
						return nil
					})).To(BeNil())
				})

				It("should store the $inc data multiple times at same minute", func() {
					storage := tsrsmgo.Storage{
						Runner: &DefaultMgoRunner,
					}
					evtTime1 := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
					err := storage.Store(&timesrs.StorageEntry{
						Event:      timesrs.NewEvent("event", nil, evtTime1),
						Collection: "aggregation1",
						Result: &timesrs.PipelineResult{
							Data: timesrs.AggregationData{
								*timesrs.NewOperationInc("field1", 1),
								*timesrs.NewOperationInc("field2", 2),
							},
						},
						Granularities: []timesrs.GranularityComposite{
							{
								Collection: timesrs.GranularityDay,
								Record:     timesrs.GranularityMinute,
							},
						},
					})

					evtTime2 := time.Date(2018, time.October, 2, 11, 47, 55, 0, time.UTC)
					err = storage.Store(&timesrs.StorageEntry{
						Event:      timesrs.NewEvent("event", nil, evtTime2),
						Collection: "aggregation1",
						Result: &timesrs.PipelineResult{
							Data: timesrs.AggregationData{
								*timesrs.NewOperationInc("field1", 3),
								*timesrs.NewOperationInc("field2", 1),
							},
						},
						Granularities: []timesrs.GranularityComposite{
							{
								Collection: timesrs.GranularityDay,
								Record:     timesrs.GranularityMinute,
							},
						},
					})
					Expect(err).To(BeNil())
					type aggregationStruct struct {
						Field1 int `bson:"field1"`
						Field2 int `bson:"field2"`
					}
					type aggregationStructDay struct {
						aggregationStruct `bson:",inline"`
						Records           map[string]aggregationStruct `bson:"records"`
					}
					Expect(DefaultMgoRunner.RunWithDB(func(db *mgo.Database) error {
						c := db.C("aggregation1_day")
						data := make([]aggregationStructDay, 0)
						Expect(c.Find(nil).All(&data)).To(BeNil())
						Expect(data).To(HaveLen(1))
						Expect(data[0].Field1).To(Equal(4))
						Expect(data[0].Field2).To(Equal(3))
						Expect(data[0].Records).To(HaveLen(1))
						Expect(data[0].Records).To(HaveKey("707"))
						Expect(data[0].Records["707"].Field1).To(Equal(4))
						Expect(data[0].Records["707"].Field2).To(Equal(3))
						return nil
					})).To(BeNil())
				})

				It("should store the $inc data multiple times", func() {
					storage := tsrsmgo.Storage{
						Runner: &DefaultMgoRunner,
					}
					evtTime1 := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
					err := storage.Store(&timesrs.StorageEntry{
						Event:      timesrs.NewEvent("event", nil, evtTime1),
						Collection: "aggregation1",
						Result: &timesrs.PipelineResult{
							Data: timesrs.AggregationData{
								*timesrs.NewOperationInc("field1", 1),
								*timesrs.NewOperationInc("field2", 2),
							},
						},
						Granularities: []timesrs.GranularityComposite{
							{
								Collection: timesrs.GranularityDay,
								Record:     timesrs.GranularityMinute,
							},
						},
					})

					evtTime2 := time.Date(2018, time.October, 2, 11, 50, 44, 0, time.UTC)
					err = storage.Store(&timesrs.StorageEntry{
						Event:      timesrs.NewEvent("event", nil, evtTime2),
						Collection: "aggregation1",
						Result: &timesrs.PipelineResult{
							Data: timesrs.AggregationData{
								*timesrs.NewOperationInc("field1", 3),
								*timesrs.NewOperationInc("field2", 1),
							},
						},
						Granularities: []timesrs.GranularityComposite{
							{
								Collection: timesrs.GranularityDay,
								Record:     timesrs.GranularityMinute,
							},
						},
					})
					Expect(err).To(BeNil())
					type aggregationStruct struct {
						Field1 int `bson:"field1"`
						Field2 int `bson:"field2"`
					}
					type aggregationStructDay struct {
						aggregationStruct `bson:",inline"`
						Records           map[string]aggregationStruct `bson:"records"`
					}
					Expect(DefaultMgoRunner.RunWithDB(func(db *mgo.Database) error {
						c := db.C("aggregation1_day")
						data := make([]aggregationStructDay, 0)
						Expect(c.Find(nil).All(&data)).To(BeNil())
						Expect(data).To(HaveLen(1))
						Expect(data[0].Field1).To(Equal(4))
						Expect(data[0].Field2).To(Equal(3))
						Expect(data[0].Records).To(HaveLen(2))
						Expect(data[0].Records["707"].Field1).To(Equal(1))
						Expect(data[0].Records["707"].Field2).To(Equal(2))
						Expect(data[0].Records["710"].Field1).To(Equal(3))
						Expect(data[0].Records["710"].Field2).To(Equal(1))
						return nil
					})).To(BeNil())
				})
			})

			When("records are hours", func() {
				It("should store the $inc data", func() {
					storage := tsrsmgo.Storage{
						Runner: &DefaultMgoRunner,
					}
					evtTime := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
					err := storage.Store(&timesrs.StorageEntry{
						Event:      timesrs.NewEvent("event", nil, evtTime),
						Collection: "aggregation1",
						Result: &timesrs.PipelineResult{
							Data: timesrs.AggregationData{
								*timesrs.NewOperationInc("field1", 1),
								*timesrs.NewOperationInc("field2", 2),
							},
						},
						Granularities: []timesrs.GranularityComposite{
							{
								Collection: timesrs.GranularityMonth,
								Record:     timesrs.GranularityHour,
							},
						},
					})
					Expect(err).To(BeNil())
					type aggregationStruct struct {
						Field1 int `bson:"field1"`
						Field2 int `bson:"field2"`
					}
					type aggregationStructDay struct {
						aggregationStruct `bson:",inline"`
						ID                time.Time                    `bson:"_id"`
						Records           map[string]aggregationStruct `bson:"records"`
					}
					Expect(DefaultMgoRunner.RunWithDB(func(db *mgo.Database) error {
						c := db.C("aggregation1_month")
						data := make([]aggregationStructDay, 0)
						Expect(c.Find(nil).All(&data)).To(BeNil())
						Expect(data).To(HaveLen(1))
						Expect(data[0].Field1).To(Equal(1))
						Expect(data[0].Field2).To(Equal(2))
						Expect(data[0].Records).To(HaveLen(1))
						Expect(data[0].Records).To(HaveKey("35"))
						Expect(data[0].Records["35"].Field1).To(Equal(1))
						Expect(data[0].Records["35"].Field2).To(Equal(2))
						return nil
					})).To(BeNil())
				})

				It("should store the $inc data multiple times at same minute", func() {
					storage := tsrsmgo.Storage{
						Runner: &DefaultMgoRunner,
					}
					evtTime1 := time.Date(2018, time.October, 2, 11, 4, 4, 0, time.UTC)
					err := storage.Store(&timesrs.StorageEntry{
						Event:      timesrs.NewEvent("event", nil, evtTime1),
						Collection: "aggregation1",
						Result: &timesrs.PipelineResult{
							Data: timesrs.AggregationData{
								*timesrs.NewOperationInc("field1", 1),
								*timesrs.NewOperationInc("field2", 2),
							},
						},
						Granularities: []timesrs.GranularityComposite{
							{
								Collection: timesrs.GranularityMonth,
								Record:     timesrs.GranularityHour,
							},
						},
					})

					evtTime2 := time.Date(2018, time.October, 2, 11, 47, 55, 0, time.UTC)
					err = storage.Store(&timesrs.StorageEntry{
						Event:      timesrs.NewEvent("event", nil, evtTime2),
						Collection: "aggregation1",
						Result: &timesrs.PipelineResult{
							Data: timesrs.AggregationData{
								*timesrs.NewOperationInc("field1", 3),
								*timesrs.NewOperationInc("field2", 1),
							},
						},
						Granularities: []timesrs.GranularityComposite{
							{
								Collection: timesrs.GranularityMonth,
								Record:     timesrs.GranularityHour,
							},
						},
					})
					Expect(err).To(BeNil())
					type aggregationStruct struct {
						Field1 int `bson:"field1"`
						Field2 int `bson:"field2"`
					}
					type aggregationStructDay struct {
						aggregationStruct `bson:",inline"`
						Records           map[string]aggregationStruct `bson:"records"`
					}
					Expect(DefaultMgoRunner.RunWithDB(func(db *mgo.Database) error {
						c := db.C("aggregation1_month")
						data := make([]aggregationStructDay, 0)
						Expect(c.Find(nil).All(&data)).To(BeNil())
						Expect(data).To(HaveLen(1))
						Expect(data[0].Field1).To(Equal(4))
						Expect(data[0].Field2).To(Equal(3))
						Expect(data[0].Records).To(HaveLen(1))
						Expect(data[0].Records).To(HaveKey("35"))
						Expect(data[0].Records["35"].Field1).To(Equal(4))
						Expect(data[0].Records["35"].Field2).To(Equal(3))
						return nil
					})).To(BeNil())
				})

				It("should store the $inc data multiple times", func() {
					storage := tsrsmgo.Storage{
						Runner: &DefaultMgoRunner,
					}
					evtTime1 := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
					err := storage.Store(&timesrs.StorageEntry{
						Event:      timesrs.NewEvent("event", nil, evtTime1),
						Collection: "aggregation1",
						Result: &timesrs.PipelineResult{
							Data: timesrs.AggregationData{
								*timesrs.NewOperationInc("field1", 1),
								*timesrs.NewOperationInc("field2", 2),
							},
						},
						Granularities: []timesrs.GranularityComposite{
							{
								Collection: timesrs.GranularityMonth,
								Record:     timesrs.GranularityHour,
							},
						},
					})

					evtTime2 := time.Date(2018, time.October, 21, 15, 34, 33, 0, time.UTC)
					err = storage.Store(&timesrs.StorageEntry{
						Event:      timesrs.NewEvent("event", nil, evtTime2),
						Collection: "aggregation1",
						Result: &timesrs.PipelineResult{
							Data: timesrs.AggregationData{
								*timesrs.NewOperationInc("field1", 3),
								*timesrs.NewOperationInc("field2", 1),
							},
						},
						Granularities: []timesrs.GranularityComposite{
							{
								Collection: timesrs.GranularityMonth,
								Record:     timesrs.GranularityHour,
							},
						},
					})
					Expect(err).To(BeNil())
					type aggregationStruct struct {
						Field1 int `bson:"field1"`
						Field2 int `bson:"field2"`
					}
					type aggregationStructDay struct {
						aggregationStruct `bson:",inline"`
						Records           map[string]aggregationStruct `bson:"records"`
					}
					Expect(DefaultMgoRunner.RunWithDB(func(db *mgo.Database) error {
						c := db.C("aggregation1_month")
						data := make([]aggregationStructDay, 0)
						Expect(c.Find(nil).All(&data)).To(BeNil())
						Expect(data).To(HaveLen(1))
						Expect(data[0].Field1).To(Equal(4))
						Expect(data[0].Field2).To(Equal(3))
						Expect(data[0].Records).To(HaveLen(2))
						Expect(data[0].Records).To(HaveKey("35"))
						Expect(data[0].Records["35"].Field1).To(Equal(1))
						Expect(data[0].Records["35"].Field2).To(Equal(2))
						Expect(data[0].Records).To(HaveKey("495"))
						Expect(data[0].Records["495"].Field1).To(Equal(3))
						Expect(data[0].Records["495"].Field2).To(Equal(1))
						return nil
					})).To(BeNil())
				})
			})
		})

		Context("with keys", func() {
			InitMgoRunner()

			It("should store the $inc data", func() {
				storage := tsrsmgo.Storage{
					Runner: &DefaultMgoRunner,
				}
				evtTime := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
				err := storage.Store(&timesrs.StorageEntry{
					Keys: map[string]interface{}{
						"keyfield1": "valuekey1",
					},
					Event:      timesrs.NewEvent("event", nil, evtTime),
					Collection: "aggregation1",
					Result: &timesrs.PipelineResult{
						Data: timesrs.AggregationData{
							*timesrs.NewOperationInc("field1", 1),
							*timesrs.NewOperationInc("field2", 2),
						},
					},
					Granularities: []timesrs.GranularityComposite{
						{
							Collection: timesrs.GranularityDay,
							Record:     timesrs.GranularitySecond,
						},
					},
				})
				Expect(err).To(BeNil())
				type aggregationStruct struct {
					At        time.Time `bson:"at"`
					KeyField1 string    `bson:"keyfield1"`
					Field1    int       `bson:"field1"`
					Field2    int       `bson:"field2"`
				}
				type aggregationStructDay struct {
					aggregationStruct `bson:",inline"`
					Records           map[string]aggregationStruct `bson:"records"`
				}
				Expect(DefaultMgoRunner.RunWithDB(func(db *mgo.Database) error {
					c := db.C("aggregation1_day")
					data := make([]aggregationStructDay, 0)
					Expect(c.Find(nil).All(&data)).To(BeNil())
					Expect(data).To(HaveLen(1))
					Expect(data[0].At).To(Equal(time.Date(2018, time.October, 2, 0, 0, 0, 0, time.UTC)))
					Expect(data[0].KeyField1).To(Equal("valuekey1"))
					Expect(data[0].Field1).To(Equal(1))
					Expect(data[0].Field2).To(Equal(2))
					Expect(data[0].Records).To(HaveLen(1))
					Expect(data[0].Records).To(HaveKey("42424"))
					Expect(data[0].Records["42424"].Field1).To(Equal(1))
					Expect(data[0].Records["42424"].Field2).To(Equal(2))
					return nil
				})).To(BeNil())
			})

			It("should store the $inc data multiple times at same second and same key", func() {
				storage := tsrsmgo.Storage{
					Runner: &DefaultMgoRunner,
				}
				evtTime := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
				err := storage.Store(&timesrs.StorageEntry{
					Keys: map[string]interface{}{
						"keyfield1": "valuekey1",
					},
					Event:      timesrs.NewEvent("event", nil, evtTime),
					Collection: "aggregation1",
					Result: &timesrs.PipelineResult{
						Data: timesrs.AggregationData{
							*timesrs.NewOperationInc("field1", 1),
							*timesrs.NewOperationInc("field2", 2),
						},
					},
					Granularities: []timesrs.GranularityComposite{
						{
							Collection: timesrs.GranularityDay,
							Record:     timesrs.GranularitySecond,
						},
					},
				})

				err = storage.Store(&timesrs.StorageEntry{
					Keys: map[string]interface{}{
						"keyfield1": "valuekey1",
					},
					Event:      timesrs.NewEvent("event", nil, evtTime),
					Collection: "aggregation1",
					Result: &timesrs.PipelineResult{
						Data: timesrs.AggregationData{
							*timesrs.NewOperationInc("field1", 3),
							*timesrs.NewOperationInc("field2", 1),
						},
					},
					Granularities: []timesrs.GranularityComposite{
						{
							Collection: timesrs.GranularityDay,
							Record:     timesrs.GranularitySecond,
						},
					},
				})
				Expect(err).To(BeNil())
				type aggregationStruct struct {
					Field1 int `bson:"field1"`
					Field2 int `bson:"field2"`
				}
				type aggregationStructDay struct {
					aggregationStruct `bson:",inline"`
					Records           map[string]aggregationStruct `bson:"records"`
				}
				Expect(DefaultMgoRunner.RunWithDB(func(db *mgo.Database) error {
					c := db.C("aggregation1_day")
					data := make([]aggregationStructDay, 0)
					Expect(c.Find(nil).All(&data)).To(BeNil())
					Expect(data).To(HaveLen(1))
					Expect(data[0].Field1).To(Equal(4))
					Expect(data[0].Field2).To(Equal(3))
					Expect(data[0].Records).To(HaveLen(1))
					Expect(data[0].Records).To(HaveKey("42424"))
					Expect(data[0].Records["42424"].Field1).To(Equal(4))
					Expect(data[0].Records["42424"].Field2).To(Equal(3))
					return nil
				})).To(BeNil())
			})

			It("should store the $inc data multiple times at same second and different key", func() {
				storage := tsrsmgo.Storage{
					Runner: &DefaultMgoRunner,
				}
				evtTime := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
				err := storage.Store(&timesrs.StorageEntry{
					Keys: map[string]interface{}{
						"keyfield1": "valuekey1",
					},
					Event:      timesrs.NewEvent("event", nil, evtTime),
					Collection: "aggregation1",
					Result: &timesrs.PipelineResult{
						Data: timesrs.AggregationData{
							*timesrs.NewOperationInc("field1", 1),
							*timesrs.NewOperationInc("field2", 2),
						},
					},
					Granularities: []timesrs.GranularityComposite{
						{
							Collection: timesrs.GranularityDay,
							Record:     timesrs.GranularitySecond,
						},
					},
				})

				err = storage.Store(&timesrs.StorageEntry{
					Keys: map[string]interface{}{
						"keyfield1": "valuekey2",
					},
					Event:      timesrs.NewEvent("event", nil, evtTime),
					Collection: "aggregation1",
					Result: &timesrs.PipelineResult{
						Data: timesrs.AggregationData{
							*timesrs.NewOperationInc("field1", 3),
							*timesrs.NewOperationInc("field2", 1),
						},
					},
					Granularities: []timesrs.GranularityComposite{
						{
							Collection: timesrs.GranularityDay,
							Record:     timesrs.GranularitySecond,
						},
					},
				})
				Expect(err).To(BeNil())
				type aggregationStruct struct {
					KeyField1 string `bson:"keyfield1"`
					Field1    int    `bson:"field1"`
					Field2    int    `bson:"field2"`
				}
				type aggregationStructDay struct {
					aggregationStruct `bson:",inline"`
					Records           map[string]aggregationStruct `bson:"records"`
				}
				Expect(DefaultMgoRunner.RunWithDB(func(db *mgo.Database) error {
					c := db.C("aggregation1_day")
					data := make([]aggregationStructDay, 0)
					Expect(c.Find(nil).All(&data)).To(BeNil())
					Expect(data).To(HaveLen(2))
					Expect(data[0].KeyField1).To(Equal("valuekey1"))
					Expect(data[0].Field1).To(Equal(1))
					Expect(data[0].Field2).To(Equal(2))
					Expect(data[0].Records).To(HaveLen(1))
					Expect(data[0].Records).To(HaveKey("42424"))
					Expect(data[0].Records["42424"].Field1).To(Equal(1))
					Expect(data[0].Records["42424"].Field2).To(Equal(2))
					Expect(data[1].KeyField1).To(Equal("valuekey2"))
					Expect(data[1].Field1).To(Equal(3))
					Expect(data[1].Field2).To(Equal(1))
					Expect(data[1].Records).To(HaveLen(1))
					Expect(data[1].Records).To(HaveKey("42424"))
					Expect(data[1].Records["42424"].Field1).To(Equal(3))
					Expect(data[1].Records["42424"].Field2).To(Equal(1))
					return nil
				})).To(BeNil())
			})
		})
	})
})
