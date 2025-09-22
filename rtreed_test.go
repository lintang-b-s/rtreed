package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/lintang-b-s/rtreed/lib/index"
	"github.com/lintang-b-s/rtreed/lib/tree"
)

func TestInsert(t *testing.T) {
	rtd, err := index.NewRtreed(2, 50, 100, 4)
	if err != nil {
		panic(err)
	}

	faker := gofakeit.New(0)

	startTimer := time.Now()
	for i := 0; i < 5e4; i++ {
		// random lat lon

		randomLat, _ := faker.LatitudeInRange(-7.818711242232534, -7.767187043571421)
		randomLon, _ := faker.LongitudeInRange(110.32382482774563, 110.42872530361015)
		point := tree.NewPoint(randomLat, randomLon)
		rtd.Insert(tree.NewSpatialData(point, []byte("coba")))
	}

	fmt.Printf("Total time to insert: %v seconds\n", time.Since(startTimer).Seconds())

	rtd.Close()

	rtdRead, err := index.NewRtreed(2, 50, 100, 4)
	if err != nil {
		panic(err)
	}

	startTimer = time.Now()
	query := tree.NewPoint(-7.767559872795658, 110.37630049924584)

	for i := 0; i < 100; i++ {
		results := rtdRead.SearchWithinRadius(query, 0.035)
		for _, res := range results {
			dist := index.HaversineDistance(query.Lat, query.Lon, res.Location().Lat, res.Location().Lon)
			if dist > 0.035*2.0 {
				t.Errorf("Data found outside radius: %v > %v", dist, 0.035)
			}
		}
	}

	rtdRead.Close()
}

// to run this benchmark, please run the test first to populate the db with data
func BenchmarkSearch(b *testing.B) {

	rtd, err := index.NewRtreed(2, 50, 100, 4)
	if err != nil {
		panic(err)
	}
	faker := gofakeit.New(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// random lat lon
		randomLat, _ := faker.LatitudeInRange(-7.818711242232534, -7.767187043571421)
		randomLon, _ := faker.LongitudeInRange(110.32382482774563, 110.42872530361015)
		point := tree.NewPoint(randomLat, randomLon)
		results := rtd.SearchWithinRadius(point, 0.035) // 35 meter radius
		if len(results) != 0 {
			_ = results
		}
	}

	b.StopTimer()
	throughput := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(throughput, "ops/sec")
}
