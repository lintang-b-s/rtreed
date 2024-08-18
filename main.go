package main

import (
	"fmt"
	"os"

	"github.com/lintang-b-s/lbs/lib/index"
	"github.com/lintang-b-s/lbs/lib/page"

	"github.com/brianvoe/gofakeit/v7"
)

type Rtree struct {
	StRtree *index.Rtree
}

func NewRtree(stR *index.Rtree) *Rtree {
	return &Rtree{
		stR,
	}
}

func concurrent() {
	options := &page.Options{
		PageSize:       os.Getpagesize(), //4096
		MinFillPercent: 0.0125,
		MaxFillPercent: 0.025,
	}

	dal, _ := page.NewDal("rtree.db", options)
	rtg := index.NewTree(2, 25, 50, dal.Root, dal.Height, dal.Size) // 25 50
	rtg.Dal = dal
	rt := NewRtree(rtg)

	faker := gofakeit.New(0)

	nearestLoc := []string{
		"ums",
		"dprd solo",
		"edupark",
	}
	visited := make(map[string]bool)
	for i := 0; i < 100000; i++ {

		var bs []byte

		// -7.5603043654535504, 110.76903599515722
		// -7.550689703596469, 110.78839081458318
		randomLat, _ := faker.LatitudeInRange(-7.5603043654535504, -7.550689703596469)
		randomLon, _ := faker.LongitudeInRange(110.76903599515722, 110.78839081458318)
		if randomLat >= -7.5572877873608 && randomLat <= -7.55060855148854 &&
			randomLon >= 110.7715659324846 && randomLon <= 110.78890373128874 {
			for _, loc := range nearestLoc {
				if !visited[loc] {
					visited[loc] = true
					bs = []byte(loc)
				}
			}
		}

		if bs == nil {
			bs = []byte("random")
		}
		rt.StRtree.Insert(index.SpatialData{
			Location: index.Point{randomLat, randomLon},
			Data:     bs,
		})
	}

	fmt.Println("pake algoritma nearest neighbor ku sendiri")
	nearestRes2 := rt.StRtree.NearestNeighbors(3, index.Point{-7.5572877873608, 110.7715659324846})
	for _, res := range nearestRes2 {
		fmt.Println(string(res.Data))
		fmt.Println(res.Location)
	}

	_ = dal.Close()
}

func sequential() {
	options := &page.Options{
		PageSize:       os.Getpagesize(), //4096
		MinFillPercent: 0.0125,
		MaxFillPercent: 0.025,
	}

	dal, _ := page.NewDal("rtree.db", options)
	rtg := index.NewTree(2, 25, 50, dal.Root, dal.Height, dal.Size) // 25 50
	rtg.Dal = dal
	rt := NewRtree(rtg)

	faker := gofakeit.New(0)

	nearestLoc := []string{
		"ums",
		"dprd solo",
		"edupark",
	}
	visited := make(map[string]bool)
	for i := 0; i < 1000; i++ {

		var bs []byte

		// -7.5603043654535504, 110.76903599515722
		// -7.550689703596469, 110.78839081458318
		randomLat, _ := faker.LatitudeInRange(-7.5603043654535504, -7.550689703596469)
		randomLon, _ := faker.LongitudeInRange(110.76903599515722, 110.78839081458318)
		if randomLat >= -7.5572877873608 && randomLat <= -7.55060855148854 &&
			randomLon >= 110.7715659324846 && randomLon <= 110.78890373128874 {
			for _, loc := range nearestLoc {
				if !visited[loc] {
					visited[loc] = true
					bs = []byte(loc)
				}
			}
		}

		if bs == nil {
			bs = []byte("random")
		}
		rt.StRtree.Insert(index.SpatialData{
			Location: index.Point{randomLat, randomLon},
			Data:     bs,
		})
	}

	fmt.Println("pake algoritma nearest neighbor ku sendiri")
	nearestRes2 := rt.StRtree.NearestNeighbors(3, index.Point{-7.5572877873608, 110.7715659324846})
	for _, res := range nearestRes2 {
		fmt.Println(string(res.Data))
		fmt.Println(res.Location)
	}

	_ = dal.Close()
}

func main() {
	sequential()
	// concurrent()
}
