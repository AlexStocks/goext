package gxring

/*
type:      [1]: end > begin
iter:     begin(2)     end(8)
            |           |
data:   _ _ * * * * * * _ _ _
buffer: _ _ _ _ _ _ _ _ _ _ _
index:  0 1 2 3 4 5 6 7 8 9 10
type:      [2] end < begin
iter:      end(2)   begin(7)
            |         |
data:   * * _ _ _ _ _ * * * *
buffer: _ _ _ _ _ _ _ _ _ _ _
index:  0 1 2 3 4 5 6 7 8 9 10
type:      [3] empty
iter:      begin(4),end(4)
                |
data:   _ _ _ _ _ _ _ _ _ _ _
buffer: _ _ _ _ _ _ _ _ _ _ _
index:  0 1 2 3 4 5 6 7 8 9 10
​
type:      [4] full
iter:      end(4),begin(5)
|               | |
data:   * * * * _ * * * * * *
buffer: _ _ _ _ _ _ _ _ _ _ _
index:  0 1 2 3 4 5 6 7 8 9 10
*/

type Ring struct {
	buffer   []byte
	capacity int // buffer capacity
	begin    int
	end      int
}

func NewRing(capacity int) *Ring {
	return &Ring{
		buffer:   make([]byte, capacity),
		capacity: capacity,
	}
}

func (b *Ring) Write(data []byte) bool {
	size := len(data)
	if b.Size()+size >= b.capacity {
		return false
	}

	if b.end >= b.begin {
		// [1][3]
		// 能装下
		if b.capacity-b.end >= size {
			copy(b.buffer[b.end:], data)
		} else {
			copy(b.buffer[b.end:], data)
			copy(b.buffer, data[(b.capacity-b.end):])
		}
	} else {
		//[2]
		copy(b.buffer[b.end:], data)
	}

	b.end = (b.end + size) % b.capacity

	return true
}

func (b *Ring) Read(data []byte) int {
	size := len(data)
	if b.Size() == 0 || size == 0 {
		return 0
	}
	if size > b.Size() {
		size = b.Size()
	}

	if b.begin >= b.end {
		// [2][4]
		// 能读完
		if b.capacity-b.begin >= size {
			copy(data, b.buffer[b.begin:b.begin+size])
		} else {
			copy(data, b.buffer[b.begin:])
			size1 := b.capacity - b.begin
			copy(data[size1:], b.buffer[0:(size-size1)])
		}
	} else {
		// [1]
		copy(data, b.buffer[b.begin:b.begin+size])
	}

	b.begin = (b.begin + size) % b.capacity

	return size
}

func (b *Ring) Clear() {
	b.begin = 0
	b.end = 0
}

func (b *Ring) Size() int {
	return (b.end + b.capacity - b.begin) % b.capacity
}

func (b *Ring) Capacity() int {
	return b.capacity
}

func (b *Ring) Empty() bool {
	return b.Size() == 0
}

func (b *Ring) Full() bool {
	return b.Size()+1 == b.Capacity()
}

func (b *Ring) FreeSize() int {
	return b.Capacity() - b.Size() - 1
}
