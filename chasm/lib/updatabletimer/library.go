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
	return nil
}

type library struct {
	componentOnlyLibrary
	handler *handler
}

func newLibrary(handler *handler) *library {
	return &library{
		componentOnlyLibrary: *newComponentOnlyLibrary(),
		handler:              handler,
	}
}

func (l *library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&updatabletimerpb.UpdatableTimerService_ServiceDesc, l.handler)
}
