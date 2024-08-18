package main

import (
	"os"
	"testing"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/lintang-b-s/lbs/lib/index"
	"github.com/lintang-b-s/lbs/lib/page"
	"github.com/lintang-b-s/lbs/lib/util"
)

// go test -v ./... --race
// tadi ngelock keseluruhan tree  dan insert  butuh 9 detik
// pake latch tiap page butuh lebih dari 30 detik buat insert  data

func TestDataRace(t *testing.T) {
	options := &page.Options{
		PageSize:       os.Getpagesize(), //4096
		MinFillPercent: 0.0125,
		MaxFillPercent: 0.025,
	}

	dal, _ := page.NewDal("rtree_test.db", options)
	rtg := index.NewTree(2, 25, 50, dal.Root, dal.Height, dal.Size) // 25 50
	rtg.Dal = dal
	rt := NewRtree(rtg)

	faker := gofakeit.New(0)

	nearestLoc := []string{
		"ums",
		"dprd solo",
		"edupark",
	}
	lennn := 10000
	items := make([]index.SpatialData, lennn)
	visited := make(map[string]bool)
	for i := 0; i < lennn; i++ {

		var bs []byte

		// -7.855728640394696, 110.2683934832971
		// -7.3129598203046555, 112.57888796447467
		randomLat, _ := faker.LatitudeInRange(-7.855728640394696, -7.3129598203046555)
		randomLon, _ := faker.LongitudeInRange(110.2683934832971, 112.57888796447467)
		if randomLat >= -7.5572877873608 && randomLat <= -7.55060855148854 &&
			randomLon >= 110.7715659324846 && randomLon <= 110.78890373128874 {
			for _, loc := range nearestLoc {
				if !visited[loc] {
					visited[loc] = true
					bs = []byte(loc)
				}
			}
		}

		items[i] = index.SpatialData{
			Location: index.Point{randomLat, randomLon},
			Data:     bs,
		}
	}

	workers := util.NewWorkerPool[index.SpatialData, interface{}](100, lennn)

	for i := 0; i < lennn; i++ {
		workers.AddJob(items[i])
	}

	close(workers.JobQueue)

	workers.Start(func(job index.SpatialData) interface{} {
		rt.StRtree.Insert(job)
		return nil
	})

	workers.Wait()

	dal.Close()
}
