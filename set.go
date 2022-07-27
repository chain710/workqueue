package workqueue

type empty struct{}
type t interface{}
type set map[t]empty

func (s set) has(item t) bool {
	_, exists := s[item]
	return exists
}

func (s set) insert(item t) {
	s[item] = empty{}
}

func (s set) delete(item t) {
	delete(s, item)
}

type valueSet map[t]t

func (s valueSet) get(key t) (interface{}, bool) {
	value, exists := s[key]
	return value, exists
}

func (s valueSet) insert(key, value t) {
	s[key] = value
}

func (s valueSet) delete(key t) (interface{}, bool) {
	value, ok := s[key]
	delete(s, key)
	return value, ok
}
