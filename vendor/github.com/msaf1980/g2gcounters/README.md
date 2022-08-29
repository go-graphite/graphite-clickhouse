# g2gcounters

Get to Graphite: counter/timer expvars for https://github.com/msaf1980/g2g

[![Build Status][1]][2]

[1]: https://secure.travis-ci.org/peterbourgon/g2g.png
[2]: http://www.travis-ci.org/peterbourgon/g2g

**See also** [g2s: Get to Statsd](https://github.com/peterbourgon/g2s), to emit
statistics to a Statsd server.

# Usage

Counter/Rate/Timer use

```go
var (
    loadedRecords = g2gcounters.NewCounter("loaded_records")
    loadedRate = g2gcounters.NewRate("loaded.rate")
    loadedERate = g2gcounters.NewRate("loaded_nonemty") // .rate is appended to metric name
    loadedTime = g2gcounters.NewTimer("loaded_time")
)

func LoadThemAll() {
    a := getSomeRecords()
    for _, x := range a {
        t := load(x)
        loadedTime.Add(t)
        loadedRate.Incr()
        loadedERate.Incr()
    }
    loadedRecords.Add(int64(len(a)))
}
```

Graphite sender

```go
func main() {

    // ...

    interval := 30 * time.Second
    timeout := 3 * time.Second
    g := g2g.NewGraphiteBatch("graphite-server:2003", interval, timeout, 4096)
    g.Register("foo.service.records.loaded", loadedRecords)
    g.Register("foo.service.records.loaded.rate", loadedRate)
    g.MRegister("foo.service.records.loaded_empty", loadedERate)
    g.MRegister("foo.service.records.load_time", loadedTime)

    // ...
}
```
