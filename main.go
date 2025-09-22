package main

import (
	"fmt"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/lintang-b-s/lbs/lib/index"
	"github.com/lintang-b-s/lbs/lib/tree"
)

func main() {
	rtd, err := index.NewRtreed(2, 25, 50)
	if err != nil {

		panic(err)
	}

	faker := gofakeit.New(0)

	startTimer := time.Now()
	// for i := 0; i < 2e5; i++ {
	// 	if (i+1)%1000 == 0 {
	// 		fmt.Printf("%v seconds for %d data\n", time.Since(startTimer).Seconds(), i+1)
	// 	}
	// 	// random lat lon
	// 	randomLat, _ := faker.LatitudeInRange(-7.818711242232534, -7.767187043571421)
	// 	randomLon, _ := faker.LongitudeInRange(110.32382482774563, 110.42872530361015)
	// 	point := tree.NewPoint(randomLat, randomLon)
	// 	rtd.Insert(tree.NewSpatialData(point, []byte("coba")))
	// }
	// rtd.Close()

	for i := 0; i < 1e2; i++ {
		if (i+1)%1000 == 0 {
			fmt.Printf("%v seconds for %d data\n", time.Since(startTimer).Seconds(), i+1)
		}
		// random lat lon
		randomLat, _ := faker.LatitudeInRange(-7.818711242232534, -7.767187043571421)
		randomLon, _ := faker.LongitudeInRange(110.32382482774563, 110.42872530361015)
		point := tree.NewPoint(randomLat, randomLon)
		results := rtd.SearchWithinRadius(point, 0.04)
		if len(results) > 0 {
			fmt.Printf("found %d results\n", len(results))
		}
	}

	fmt.Printf("%v seconds", time.Since(startTimer).Seconds())

}
