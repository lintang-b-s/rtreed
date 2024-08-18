package index

type Point struct {
	Lat float64
	Lon float64
}

func NewRect(p Point, lengths []float64) (r Rect, err error) {

	r.t.Lat = p.Lat + lengths[0]
	r.t.Lon = p.Lon + lengths[1]
	return
}

func (p Point) minDist(r Rect) float64 {

	sum := 0.0

	if p.Lat < r.s.Lat {
		sum += (p.Lat - r.s.Lat) * (p.Lat - r.s.Lat)
	} else if p.Lat > r.t.Lat {
		sum += (p.Lat - r.t.Lat) * (p.Lat - r.t.Lat)
	}

	if p.Lon < r.s.Lon {
		sum += (p.Lon - r.s.Lon) * (p.Lon - r.s.Lon)
	} else if p.Lon > r.t.Lon {
		sum += (p.Lon - r.t.Lon) * (p.Lon - r.t.Lon)
	}

	return sum
}

type Rect struct {
	s, t Point
}

func (r Rect) Equal(other Rect) bool {

	if r.s.Lat != other.s.Lat || r.s.Lon != other.s.Lon {
		return false
	}

	if r.t.Lat != other.t.Lat || r.t.Lon != other.t.Lon {
		return false
	}

	return true
}

func (r Rect) Area() float64 {
	size := 1.0

	size *= r.t.Lat - r.s.Lat
	size *= r.t.Lon - r.s.Lon
	return size
}

func (r Rect) containRect(r2 Rect) bool {

	if r.s.Lat > r2.s.Lat || r2.t.Lat > r.t.Lat {
		return false
	}

	if r.s.Lon > r2.s.Lon || r2.t.Lon > r.t.Lon {
		return false
	}

	return true
}

func (p Point) ToRect(tol float64) Rect {

	r := Rect{}
	r.s.Lat = p.Lat - tol
	r.s.Lon = p.Lon - tol
	r.t.Lat = p.Lat + tol
	r.t.Lon = p.Lon + tol

	return r
}

func createRectangle(r1, r2 Rect) (bb Rect) {
	// buat rectangle yg include r1 & r2

	if r1.s.Lat <= r2.s.Lat {
		bb.s.Lat = r1.s.Lat
	} else {
		bb.s.Lat = r2.s.Lat
	}

	if r1.s.Lon <= r2.s.Lon {
		bb.s.Lon = r1.s.Lon
	} else {
		bb.s.Lon = r2.s.Lon
	}

	if r2.t.Lat >= r1.t.Lat {
		bb.t.Lat = r2.t.Lat
	} else {
		bb.t.Lat = r1.t.Lat
	}

	if r2.t.Lon >= r1.t.Lon {
		bb.t.Lon = r2.t.Lon
	} else {
		bb.t.Lon = r1.t.Lon
	}

	return
}
