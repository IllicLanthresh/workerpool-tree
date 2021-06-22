package trees

type AnyType interface{}

type childResult struct {
	child  node
	result AnyType
}

type node interface {
	Calculate() AnyType
}

type ValueNode struct {
	value        map[DataIdentifier]AnyType
	parent       OperationNode
	childResults map[DataIdentifier][]childResult
	nodeName     string
}

type WorkerFunction func(...childResult) AnyType

type DataIdentifier string

type OperationNode struct {
	value        map[DataIdentifier]AnyType
	operation    WorkerFunction
	childs       []node
	parent       node
	childResults map[DataIdentifier][]childResult
	nodeName     string
}

func (n ValueNode) Calculate() AnyType {
	panic("implement me")
}

func (n OperationNode) Calculate() AnyType {
	panic("implement me")
}
