package updatabletimer

import (
	"go.temporal.io/server/chasm"
	updatabletimerpb "go.temporal.io/server/chasm/lib/updatabletimer/gen/updatabletimerpb/v1"
	"google.golang.org/grpc"
)

const (
	libraryName   = "updatabletimer"
	componentName = "updatabletimer"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

type componentOnlyLibrary struct {
	chasm.UnimplementedLibrary
}

func newComponentOnlyLibrary() *componentOnlyLibrary {
	return &componentOnlyLibrary{}
}

func (l *componentOnlyLibrary) Name() string {
	return libraryName
}

func (l *componentOnlyLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*UpdatableTimer](
			componentName,
			chasm.WithSearchAttributes(
				StatusSearchAttribute,
			),
			chasm.WithBusinessIDAlias("TimerId"),
		),
	}
}

type library struct {
	componentOnlyLibrary
	handler              *handler
	deadlineTaskHandler  *deadlineTaskHandler
}

func newLibrary(handler *handler, deadlineTaskHandler *deadlineTaskHandler) *library {
	return &library{
		componentOnlyLibrary: *newComponentOnlyLibrary(),
		handler:              handler,
		deadlineTaskHandler:  deadlineTaskHandler,
	}
}

func (l *library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&updatabletimerpb.UpdatableTimerService_ServiceDesc, l.handler)
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask(
			"deadline",
			l.deadlineTaskHandler,
		),
	}
}
