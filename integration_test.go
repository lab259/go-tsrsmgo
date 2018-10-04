package tsrsmgo_test

import (
	"github.com/globalsign/mgo"
	"github.com/lab259/go-timeseries"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"

	"."
)

var _ = Describe("Time Series", func() {
	Describe("Pipeline", func() {
		InitMgoRunner()

		When("dealing with inc", func() {
			It("should aggregate data", func() {
				storage := &tsrsmgo.Storage{
					Runner: &DefaultMgoRunner,
				}
				pipeline := timesrs.Pipeline{
					Storage:    storage,
					Collection: "aggregation1",
					Aggregations: []timesrs.Aggregation{
						timesrs.NewAggregationInc("field1"),
					},
					TagFnc: timesrs.NoTags,
					Granularities: []timesrs.GranularityComposite{
						{
							Collection: timesrs.GranularityDay,
							Record:     timesrs.GranularityMinute,
						},
					},
				}
				evtTime := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
				event := timesrs.NewEvent("event", nil, evtTime)
				pipelineResult, err := pipeline.Run(event)
				Expect(err).To(BeNil())
				Expect(pipelineResult.Data).To(HaveLen(1))
				Expect(pipelineResult.Data[0].Field).To(Equal("field1"))
				Expect(pipelineResult.Data[0].Value).To(Equal(1))
				type aggregationStruct struct {
					Field1 int `bson:"field1"`
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
					Expect(data[0].Records).To(HaveLen(1))
					Expect(data[0].Records).To(HaveKey("707"))
					Expect(data[0].Records["707"].Field1).To(Equal(1))
					return nil
				})).To(BeNil())
			})

			It("should aggregate data multiple times at the same record granularity", func() {
				storage := &tsrsmgo.Storage{
					Runner: &DefaultMgoRunner,
				}
				pipeline := timesrs.Pipeline{
					Storage:    storage,
					Collection: "aggregation1",
					Aggregations: []timesrs.Aggregation{
						timesrs.NewAggregationInc("field1"),
					},
					TagFnc: timesrs.NoTags,
					Granularities: []timesrs.GranularityComposite{
						{
							Collection: timesrs.GranularityDay,
							Record:     timesrs.GranularityMinute,
						},
					},
				}
				evtTime1 := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
				event := timesrs.NewEvent("event", nil, evtTime1)
				pipelineResult, err := pipeline.Run(event)
				Expect(err).To(BeNil())
				Expect(pipelineResult.Data).To(HaveLen(1))
				Expect(pipelineResult.Data[0].Field).To(Equal("field1"))
				Expect(pipelineResult.Data[0].Value).To(Equal(1))

				evtTime2 := time.Date(2018, time.October, 2, 11, 47, 55, 0, time.UTC)
				event = timesrs.NewEvent("event", nil, evtTime2)
				pipelineResult, err = pipeline.Run(event)
				Expect(err).To(BeNil())
				Expect(pipelineResult.Data).To(HaveLen(1))
				Expect(pipelineResult.Data[0].Field).To(Equal("field1"))
				Expect(pipelineResult.Data[0].Value).To(Equal(1))
				type aggregationStruct struct {
					Field1 int `bson:"field1"`
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
					Expect(data[0].Field1).To(Equal(2))
					Expect(data[0].Records).To(HaveLen(1))
					Expect(data[0].Records).To(HaveKey("707"))
					Expect(data[0].Records["707"].Field1).To(Equal(2))
					return nil
				})).To(BeNil())
			})

			It("should aggregate data multiple times in different record granularity", func() {
				storage := &tsrsmgo.Storage{
					Runner: &DefaultMgoRunner,
				}
				pipeline := timesrs.Pipeline{
					Storage:    storage,
					Collection: "aggregation1",
					Aggregations: []timesrs.Aggregation{
						timesrs.NewAggregationInc("field1", 1),
					},
					TagFnc: timesrs.NoTags,
					Granularities: []timesrs.GranularityComposite{
						{
							Collection: timesrs.GranularityDay,
							Record:     timesrs.GranularityMinute,
						},
					},
				}
				evtTime1 := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
				event := timesrs.NewEvent("event", nil, evtTime1)
				pipelineResult, err := pipeline.Run(event)
				Expect(err).To(BeNil())
				Expect(pipelineResult.Data).To(HaveLen(1))
				Expect(pipelineResult.Data[0].Field).To(Equal("field1"))
				Expect(pipelineResult.Data[0].Value).To(Equal(1))

				evtTime2 := time.Date(2018, time.October, 2, 11, 50, 55, 0, time.UTC)
				event = timesrs.NewEvent("event", nil, evtTime2)
				pipelineResult, err = pipeline.Run(event)
				Expect(err).To(BeNil())
				Expect(pipelineResult.Data).To(HaveLen(1))
				Expect(pipelineResult.Data[0].Field).To(Equal("field1"))
				Expect(pipelineResult.Data[0].Value).To(Equal(1))
				type aggregationStruct struct {
					Field1 int `bson:"field1"`
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
					Expect(data[0].Field1).To(Equal(2))
					Expect(data[0].Records).To(HaveLen(2))
					Expect(data[0].Records).To(HaveKey("707"))
					Expect(data[0].Records["707"].Field1).To(Equal(1))
					Expect(data[0].Records).To(HaveKey("707"))
					Expect(data[0].Records["710"].Field1).To(Equal(1))
					return nil
				})).To(BeNil())
			})
		})

		When("dealing with $set", func() {
			It("should aggregate data", func() {
				storage := &tsrsmgo.Storage{
					Runner: &DefaultMgoRunner,
				}
				pipeline := timesrs.Pipeline{
					Storage:    storage,
					Collection: "aggregation1",
					Aggregations: []timesrs.Aggregation{
						timesrs.NewAggregationFnc(func(e timesrs.Event, data *timesrs.AggregationData) error {
							*data = append(*data, *timesrs.NewOperationSet("field1", 5))
							return nil
						}),
					},
					TagFnc: timesrs.NoTags,
					Granularities: []timesrs.GranularityComposite{
						{
							Collection: timesrs.GranularityDay,
							Record:     timesrs.GranularityMinute,
						},
					},
				}
				evtTime := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
				event := timesrs.NewEvent("event", nil, evtTime)
				pipelineResult, err := pipeline.Run(event)
				Expect(err).To(BeNil())
				Expect(pipelineResult.Data).To(HaveLen(1))
				Expect(pipelineResult.Data[0].Type).To(Equal(timesrs.OperationTypeSet))
				Expect(pipelineResult.Data[0].Field).To(Equal("field1"))
				Expect(pipelineResult.Data[0].Value).To(Equal(5))
				type aggregationFieldSet struct {
					Value int `bson:"value"`
					Total int `bson:"total"`
					Count int `bson:"count"`
				}
				type aggregationStruct struct {
					Field1 aggregationFieldSet `bson:"field1"`
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
					Expect(data[0].Field1.Value).To(Equal(5))
					Expect(data[0].Field1.Total).To(Equal(5))
					Expect(data[0].Field1.Count).To(Equal(1))
					Expect(data[0].Records).To(HaveLen(1))
					Expect(data[0].Records).To(HaveKey("707"))
					Expect(data[0].Records["707"].Field1.Value).To(Equal(5))
					Expect(data[0].Records["707"].Field1.Total).To(Equal(5))
					Expect(data[0].Records["707"].Field1.Count).To(Equal(1))
					return nil
				})).To(BeNil())
			})

			It("should aggregate data multiple times at the same record granularity", func() {
				storage := &tsrsmgo.Storage{
					Runner: &DefaultMgoRunner,
				}
				pipeline := timesrs.Pipeline{
					Storage:    storage,
					Collection: "aggregation1",
					Aggregations: []timesrs.Aggregation{
						timesrs.NewAggregationFnc(func(e timesrs.Event, data *timesrs.AggregationData) error {
							*data = append(*data, *timesrs.NewOperationSet("field1", e.Data().(map[string]interface{})["field1"].(int)))
							return nil
						}),
					},
					TagFnc: timesrs.NoTags,
					Granularities: []timesrs.GranularityComposite{
						{
							Collection: timesrs.GranularityDay,
							Record:     timesrs.GranularityMinute,
						},
					},
				}
				evtTime1 := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
				event := timesrs.NewEvent("event", map[string]interface{}{"field1": 3}, evtTime1)
				pipelineResult, err := pipeline.Run(event)
				Expect(err).To(BeNil())
				Expect(pipelineResult.Data).To(HaveLen(1))
				Expect(pipelineResult.Data[0].Type).To(Equal(timesrs.OperationTypeSet))
				Expect(pipelineResult.Data[0].Field).To(Equal("field1"))
				Expect(pipelineResult.Data[0].Value).To(Equal(3))

				evtTime2 := time.Date(2018, time.October, 2, 11, 47, 55, 0, time.UTC)
				event = timesrs.NewEvent("event", map[string]interface{}{"field1": 5}, evtTime2)
				pipelineResult, err = pipeline.Run(event)
				Expect(err).To(BeNil())
				Expect(pipelineResult.Data).To(HaveLen(1))
				Expect(pipelineResult.Data[0].Type).To(Equal(timesrs.OperationTypeSet))
				Expect(pipelineResult.Data[0].Field).To(Equal("field1"))
				Expect(pipelineResult.Data[0].Value).To(Equal(5))
				type aggregationFieldSet struct {
					Value int `bson:"value"`
					Total int `bson:"total"`
					Count int `bson:"count"`
				}
				type aggregationStruct struct {
					Field1 aggregationFieldSet `bson:"field1"`
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
					Expect(data[0].Field1.Value).To(Equal(5))
					Expect(data[0].Field1.Total).To(Equal(8))
					Expect(data[0].Field1.Count).To(Equal(2))
					Expect(data[0].Records).To(HaveLen(1))
					Expect(data[0].Records).To(HaveKey("707"))
					Expect(data[0].Records["707"].Field1.Value).To(Equal(5))
					Expect(data[0].Records["707"].Field1.Total).To(Equal(8))
					Expect(data[0].Records["707"].Field1.Count).To(Equal(2))
					return nil
				})).To(BeNil())
			})

			It("should aggregate data multiple times in different record granularity", func() {
				storage := &tsrsmgo.Storage{
					Runner: &DefaultMgoRunner,
				}
				pipeline := timesrs.Pipeline{
					Storage:    storage,
					Collection: "aggregation1",
					Aggregations: []timesrs.Aggregation{
						timesrs.NewAggregationFnc(func(e timesrs.Event, data *timesrs.AggregationData) error {
							*data = append(*data, *timesrs.NewOperationSet("field1", e.Data().(map[string]interface{})["field1"].(int)))
							return nil
						}),
					},
					TagFnc: timesrs.NoTags,
					Granularities: []timesrs.GranularityComposite{
						{
							Collection: timesrs.GranularityDay,
							Record:     timesrs.GranularityMinute,
						},
					},
				}
				evtTime1 := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
				event := timesrs.NewEvent("event", map[string]interface{}{"field1": 3}, evtTime1)
				pipelineResult, err := pipeline.Run(event)
				Expect(err).To(BeNil())
				Expect(pipelineResult.Data).To(HaveLen(1))
				Expect(pipelineResult.Data[0].Type).To(Equal(timesrs.OperationTypeSet))
				Expect(pipelineResult.Data[0].Field).To(Equal("field1"))
				Expect(pipelineResult.Data[0].Value).To(Equal(3))

				evtTime2 := time.Date(2018, time.October, 2, 11, 50, 55, 0, time.UTC)
				event = timesrs.NewEvent("event", map[string]interface{}{"field1": 5}, evtTime2)
				pipelineResult, err = pipeline.Run(event)
				Expect(err).To(BeNil())
				Expect(pipelineResult.Data).To(HaveLen(1))
				Expect(pipelineResult.Data[0].Type).To(Equal(timesrs.OperationTypeSet))
				Expect(pipelineResult.Data[0].Field).To(Equal("field1"))
				Expect(pipelineResult.Data[0].Value).To(Equal(5))
				type aggregationFieldSet struct {
					Value int `bson:"value"`
					Total int `bson:"total"`
					Count int `bson:"count"`
				}
				type aggregationStruct struct {
					Field1 aggregationFieldSet `bson:"field1"`
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
					Expect(data[0].Field1.Value).To(Equal(5))
					Expect(data[0].Field1.Total).To(Equal(8))
					Expect(data[0].Field1.Count).To(Equal(2))
					Expect(data[0].Records).To(HaveLen(2))
					Expect(data[0].Records).To(HaveKey("707"))
					Expect(data[0].Records["707"].Field1.Value).To(Equal(3))
					Expect(data[0].Records["707"].Field1.Total).To(Equal(3))
					Expect(data[0].Records["707"].Field1.Count).To(Equal(1))
					Expect(data[0].Records).To(HaveKey("710"))
					Expect(data[0].Records["710"].Field1.Value).To(Equal(5))
					Expect(data[0].Records["710"].Field1.Total).To(Equal(5))
					Expect(data[0].Records["710"].Field1.Count).To(Equal(1))
					return nil
				})).To(BeNil())
			})
		})

		It("should fail aggregating a not supported type", func() {
			storage := &tsrsmgo.Storage{
				Runner: &DefaultMgoRunner,
			}
			pipeline := timesrs.Pipeline{
				Storage:    storage,
				Collection: "aggregation1",
				Aggregations: []timesrs.Aggregation{
					timesrs.NewAggregationFnc(func(e timesrs.Event, data *timesrs.AggregationData) error {
						*data = append(*data, timesrs.Operation{
							Type:  timesrs.OperationType(12345),
							Field: "field1",
							Value: 1,
						})
						return nil
					}),
				},
				TagFnc: timesrs.NoTags,
				Granularities: []timesrs.GranularityComposite{
					{
						Collection: timesrs.GranularityDay,
						Record:     timesrs.GranularityMinute,
					},
				},
			}
			evtTime1 := time.Date(2018, time.October, 2, 11, 47, 4, 0, time.UTC)
			event := timesrs.NewEvent("event", map[string]interface{}{"field1": 3}, evtTime1)
			pipelineResult, err := pipeline.Run(event)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring("not supported"))
			Expect(pipelineResult).To(BeNil())
		})
	})
})
