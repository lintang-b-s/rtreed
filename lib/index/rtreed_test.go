package index

import (
	"testing"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/lintang-b-s/lbs/lib/tree"
)

func TestRtreedInsert(t *testing.T) {
	rtg, err := NewRtreed(2, 25, 50)
	if err != nil {
		t.Errorf("Error creating rtreed: %s", err)
	}
	faker := gofakeit.New(0)

	for i := 0; i < 17000; i++ {
		randomLat, _ := faker.LatitudeInRange(-7.818711242232534, -7.767187043571421)
		randomLon, _ := faker.LongitudeInRange(110.32382482774563, 110.42872530361015)
		point := tree.NewPoint(randomLat, randomLon)
		rtg.Insert(tree.NewSpatialData(point, []byte("coba")))
	}
}
