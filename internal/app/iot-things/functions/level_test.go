package functions

import (
	"testing"
	"time"

	"github.com/matryer/is"
)

func TestLevel(t *testing.T) {
	is := is.New(t)

	angle := 0.0
	maxDistance := 1.0
	maxLevel := 1.0
	meanLevel := 0.0
	offset := 0.0

	lvl, err := NewLevel(&angle, &maxDistance, &maxLevel, &meanLevel, &offset, 0.0)
	is.NoErr(err)

	b, err := lvl.Calc(0.5, time.Now())
	is.NoErr(err)
	is.True(b)
	is.Equal(0.5, lvl.Current())
	is.Equal(float64(50), lvl.Percent())
}

func TestLevelNegative(t *testing.T) {
	is := is.New(t)

	angle := 0.0
	maxDistance := 1.61
	maxLevel := 1.61
	meanLevel := 0.0
	offset := 0.0

	lvl, err := NewLevel(&angle, &maxDistance, &maxLevel, &meanLevel, &offset, 0.0)
	is.NoErr(err)

	values := [][]float64{
		{1.64447, -0.03447},
		{1.6418, -0.0318},
		{1.63988, -0.02988},
		{1.64072, -0.03072},
		{1.6417, -0.0317},
	}

	for _, v := range values {
		b, err := lvl.Calc(v[0], time.Now())
		is.NoErr(err)
		is.True(b)
		is.Equal(v[1], lvl.Current())
	}
}

func TestLevelPositive(t *testing.T) {
	is := is.New(t)

	angle := 0.0
	maxDistance := 3.3
	maxLevel := 3.3
	meanLevel := 0.0
	offset := 0.0

	lvl, err := NewLevel(&angle, &maxDistance, &maxLevel, &meanLevel, &offset, 0.0)
	is.NoErr(err)

	values := [][]float64{
		{3.07254, 0.22746},
		{3.07388, 0.22612},
	}

	for _, v := range values {
		b, err := lvl.Calc(v[0], time.Now())
		is.NoErr(err)
		is.True(b)
		is.Equal(v[1], lvl.Current())
	}
}

func TestLevelWithOffset(t *testing.T) {
	is := is.New(t)

	angle := 0.0
	maxDistance := 3.0
	maxLevel := 3.0
	meanLevel := 0.0
	offset := 1.0

	lvl, err := NewLevel(&angle, &maxDistance, &maxLevel, &meanLevel, &offset, 0.0)
	is.NoErr(err)

	values := [][]float64{
		{1, 2},
		{1.5, 1.5},
		{2.5, 1},
	}

	for _, v := range values {
		b, err := lvl.Calc(v[0], time.Now())
		is.NoErr(err)
		is.True(b)
		is.Equal(v[1], lvl.Current())
	}
}
