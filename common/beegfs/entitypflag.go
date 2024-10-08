package beegfs

// Used for reading in a EntityId of various type from cobra or pflag Flag. Implements pflag.Value.
type EntityIdPFlag struct {
	parser EntityIdParser
	into   *EntityId
}

// The returned pointer can be passed to cobra.Command.Flags().Var() to read in an entity id
// from the user. into specifies where the parsed input shall be written to and also provides the
// default value. idBitSize sets the allowed numeric id range.
func NewEntityIdPFlag(into *EntityId, idBitSize int, accepted ...NodeType) *EntityIdPFlag {
	return &EntityIdPFlag{
		parser: NewEntityIdParser(idBitSize, accepted...),
		into:   into,
	}
}

// Implement pflag.Value
func (g *EntityIdPFlag) Type() string {
	return "entityId"
}

// Implement pflag.Value
func (g *EntityIdPFlag) String() string {
	if g.into != nil {
		return (*g.into).String()
	}
	return "unspecified"
}

// Implement pflag.value
func (g *EntityIdPFlag) Set(input string) error {
	r, err := g.parser.Parse(input)
	if err != nil {
		return err
	}
	*g.into = r
	return nil
}

type EntityIdSlicePFlag struct {
	parser EntityIdSliceParser
	into   *[]EntityId
}

// The returned pointer can be passed to cobra.Command.Flags().Var() to read in multiple entity IDs
// separated by commas from the user. The into parameter specifies where parsed input is written and
// also provides the default value. The idBitSize defines the allowed numeric ID range.
func NewEntityIdSlicePFlag(into *[]EntityId, idBitSize int, accepted ...NodeType) *EntityIdSlicePFlag {
	return &EntityIdSlicePFlag{
		parser: NewEntityIdSliceParser(idBitSize, accepted...),
		into:   into,
	}
}

func (p *EntityIdSlicePFlag) Type() string {
	return "<entityId>, [<entityId>]..."
}

func (p *EntityIdSlicePFlag) String() string {
	return "unspecified"
}

func (p *EntityIdSlicePFlag) Set(input string) error {
	r, err := p.parser.Parse(input)
	if err != nil {
		return err
	}
	*p.into = r
	return nil
}
