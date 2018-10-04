# tsrsmgo

## Getting started

tsrsmgo is the MongoDB storage implementation for the
[go-timeseries](http://github.com/lab259/go-timeseries).

This project implements two types of operation: $inc (`timesrs.OperationTypeInc`)
and $set (`timesrs.OperationTypeSet`).

### $inc

This operation will increment the given field by the `value` of the
operation (normally 1).

### $set

This operation is a bit more complex of the $inc version. For each
value, it will create an object that will keep:

```js
{
    value: 1, // The current value
    total: 3, // The sum of all values
    count: 2, // The number of events captured in this granularity
}
```

This structure is replicated both in the `collection` and `records`
entries.

## Example

### Simple increment

The below example will aggregate the number of reads from a sensor. The
data will be stored in a collection called `sensor_data_day`.

Each document in the sensor data will represent a day. Each day will
have `records` that will represent an aggregation of one minute. The
structure of the document will be as shown below:

```js
{
	"_id" : ISODate("2018-10-02T00:00:00Z"),
	"read" : 1454, // Total of reads of the day
	"records" : {
		"0" : {          // Minute 0 of this day
			"read" : 2
		},
		"1" : {          // Minute 1 of this day
			"read" : 1
		},
		"2" : {          // Minute 2 of this day
			"read" : 4
		},
		// ...
	}
}
```

**Setup**

```go
// ...
storage := &tsrsmgo.Storage{
    Runner: &DefaultMgoRunner,
}
pipeline := timesrs.Pipeline{
    Storage:    storage,
    Collection: "sensor_data",
    Aggregations: []timesrs.Aggregation{
        timesrs.NewAggregationInc("read"),
    },
    TagFnc: timesrs.NoTags,
    Granularities: []timesrs.GranularityComposite{
        {
            Collection: timesrs.GranularityDay,
            Record:     timesrs.GranularityMinute,
        },
    },
}
// ...
```

**Triggering an event**

```
// This event will not have any data
event := timesrs.NewEvent("read_data_from_sensor", nil, timesrs.DefaultClock.Time())
pipelineResult, err := pipeline.Run(event)
```

## Development

### Requirements

* docker;
* docker-compose;

### Dependencies

To download all dependencies use the following command.

    $ make deps

### Developer environment

We use the `docker-compose` to configure the mongo server for testing.

    $ make dco-test-up

### Testing

In order to run all tests use:

    $ make test

If you do prefer to watch file changes an run tests automatically while
developing, use:

    $ make test-watch

## License

MIT License