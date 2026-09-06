package queue

type Node struct {
	Element string
	Next    *Node
}

type Queue struct {
	Root    *Node
	Counter int64
}

func NewQueue() *Queue {
	q := &Queue{
		Root:    nil,
		Counter: 0,
	}
	return q
}

func (q *Queue) Push(i string) {
	q.Counter++
	nd := &Node{Element: i, Next: nil}
	if q.Root == nil {
		q.Root = nd
		return
	}
	start := q.Root
	for start.Next != nil {
		start = start.Next
	}
	start.Next = nd
}

func (q *Queue) Pop() string {
	first := q.Root.Element
	q.Root = q.Root.Next
	q.Counter--
	return first
}

func (q *Queue) Count() int64 {
	return q.Counter
}
