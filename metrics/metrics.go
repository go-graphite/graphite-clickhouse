package metrics

import (
	"github.com/msaf1980/g2g"
	"github.com/msaf1980/g2gcounters"
)

type CacheMetric struct {
	CacheHits   *g2gcounters.UInt
	CacheMisses *g2gcounters.UInt
}

var FinderCacheMetrics *CacheMetric
var ShortCacheMetrics *CacheMetric
var DefaultCacheMetrics *CacheMetric

func InitFindCacheMetrics(g *g2g.Graphite, prefix string) {
	FinderCacheMetrics = &CacheMetric{
		CacheHits:   g2gcounters.NewUInt("find_cache_hits"),
		CacheMisses: g2gcounters.NewUInt("find_cache_misses"),
	}

	ShortCacheMetrics = &CacheMetric{
		CacheHits:   g2gcounters.NewUInt("short_cache_hits"),
		CacheMisses: g2gcounters.NewUInt("short_cache_misses"),
	}
	DefaultCacheMetrics = &CacheMetric{
		CacheHits:   g2gcounters.NewUInt("default_cache_hits"),
		CacheMisses: g2gcounters.NewUInt("default_cache_misses"),
	}

	if g != nil {
		g.Register(prefix+".find_cache_hits", FinderCacheMetrics.CacheHits)
		g.Register(prefix+".find_cache_misses", FinderCacheMetrics.CacheMisses)
		g.Register(prefix+".short_cache_hits", ShortCacheMetrics.CacheHits)
		g.Register(prefix+".short_cache_misses", ShortCacheMetrics.CacheMisses)
		g.Register(prefix+".default_cache_hits", DefaultCacheMetrics.CacheHits)
		g.Register(prefix+".default_cache_misses", DefaultCacheMetrics.CacheMisses)
	}
}
