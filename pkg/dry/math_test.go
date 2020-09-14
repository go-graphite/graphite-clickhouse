package dry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMax(t *testing.T) {
	assert := assert.New(t)

	assert.Equal(int64(1), Max(1, -1))
	assert.Equal(int64(2), Max(1, 2))
	assert.Equal(int64(3), Max(3, 3))
}

func TestMin(t *testing.T) {
	assert := assert.New(t)

	assert.Equal(int64(-1), Min(1, -1))
	assert.Equal(int64(1), Min(1, 2))
	assert.Equal(int64(3), Min(3, 3))
}

func TestCeilToMultiplier(t *testing.T) {
	assert := assert.New(t)

	assert.Equal(int64(0), CeilToMultiplier(1, -1))
	assert.Equal(int64(2), CeilToMultiplier(1, 2))
	assert.Equal(int64(6), CeilToMultiplier(4, 3))
	assert.Equal(int64(6), CeilToMultiplier(6, 3))
}

func TestGCD(t *testing.T) {
	assert := assert.New(t)

	assert.Equal(int64(1), GCD(1, -1))
	assert.Equal(int64(1), GCD(-1, 1))
	assert.Equal(int64(1), GCD(-1, -1))
	assert.Equal(int64(1), GCD(1, 2))
	assert.Equal(int64(1), GCD(4, 3))
	assert.Equal(int64(3), GCD(6, 3))
}

func TestLCM(t *testing.T) {
	assert := assert.New(t)

	assert.Equal(int64(1), LCM(1, -1))
	assert.Equal(int64(1), LCM(-1, 1))
	assert.Equal(int64(1), LCM(-1, -1))
	assert.Equal(int64(2), LCM(1, 2))
	assert.Equal(int64(6), LCM(6, 3))
	assert.Equal(int64(12), LCM(4, 3))
}
