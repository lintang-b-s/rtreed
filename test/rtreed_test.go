package test

import (
	"fmt"
	"testing"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/lintang-b-s/lbs/lib/index"
	"github.com/lintang-b-s/lbs/lib/tree"
)

type Rtree struct {
	StRtree *index.Rtreed
}

func NewRtree(stR *index.Rtreed) *Rtree {
	return &Rtree{
		stR,
	}
}

func BenchmarkInsert(b *testing.B) {

	rtg, err := index.NewRtreed(2, 25, 50)
	if err != nil {
		b.Errorf("Error creating rtreed: %s", err)
	}
	rt := NewRtree(rtg)

	faker := gofakeit.New(0)

	b.ResetTimer()

	for i := 0; i < 1000; i++ {
		// random lat lon
		if i == 999 {
			fmt.Printf("debug")
		}
		randomLat, _ := faker.LatitudeInRange(-7.818711242232534, -7.767187043571421)
		randomLon, _ := faker.LongitudeInRange(110.32382482774563, 110.42872530361015)
		point := tree.NewPoint(randomLat, randomLon)
		rt.StRtree.Insert(tree.NewSpatialData(point, []byte("coba")))
	}

	b.StopTimer()
	rtg.Close()

	// rtg2, err := index.NewRtreed(2, 25, 50)
	// if err != nil {
	// 	b.Errorf("Error creating rtreed: %s", err)
	// }
	// rt2 := NewRtree(rtg2)

	// faker2 := gofakeit.New(0)
	// b.ResetTimer()

	// for i := 0; i < b.N; i++ {
	// 	// random lat lon
	// 	randomLat, _ := faker2.LatitudeInRange(-7.818711242232534, -7.767187043571421)
	// 	randomLon, _ := faker2.LongitudeInRange(110.32382482774563, 110.42872530361015)
	// 	point := tree.NewPoint(randomLat, randomLon)
	// 	results := rt2.StRtree.SearchWithinRadius(point, 2.0)
	// 	if len(results) != 0 {
	// 		_ = results
	// 	}
	// }
	// b.StopTimer()
}

func BenchmarkSearch(b *testing.B) {

	rtg, err := index.NewRtreed(2, 25, 50)
	if err != nil {
		b.Errorf("Error creating rtreed: %s", err)
	}
	rt := NewRtree(rtg)

	faker := gofakeit.New(0)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// random lat lon
		randomLat, _ := faker.LatitudeInRange(-7.818711242232534, -7.767187043571421)
		randomLon, _ := faker.LongitudeInRange(110.32382482774563, 110.42872530361015)
		point := tree.NewPoint(randomLat, randomLon)
		results := rt.StRtree.SearchWithinRadius(point, 0.04) // 40 meter radius
		if len(results) != 0 {
			_ = results
		}
	}

	b.StopTimer()
	throughput := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(throughput, "ops/sec")
}
