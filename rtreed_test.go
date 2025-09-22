package main

import (
	"testing"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/lintang-b-s/lbs/lib/index"
	"github.com/lintang-b-s/lbs/lib/tree"
)

func BenchmarkSearch(b *testing.B) {

	rtg, err := index.NewRtreed(2, 25, 50)
	if err != nil {
		b.Errorf("Error creating rtreed: %s", err)
	}

	faker := gofakeit.New(0)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// random lat lon
		randomLat, _ := faker.LatitudeInRange(-7.818711242232534, -7.767187043571421)
		randomLon, _ := faker.LongitudeInRange(110.32382482774563, 110.42872530361015)
		point := tree.NewPoint(randomLat, randomLon)
		results := rtg.SearchWithinRadius(point, 0.04) // 40 meter radius
		if len(results) != 0 {
			_ = results
		}
	}

	b.StopTimer()
	throughput := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(throughput, "ops/sec")
}
