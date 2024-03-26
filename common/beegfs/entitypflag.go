package beegfs

// Used for reading in a EntityId of various type from cobra or pflag Flag. Implements pflag.Value.
type EntityPFlag struct {
	parser   EntityParser
	typeName string
	into     *EntityId
}

// The returned pointer can be passed to cobra.Command.Flags().Var() to read in an node id from the
// user. into specifies where the parsed input shall be written to and also provides the default
// value.
func NewNodePFlag(into *EntityId) *EntityPFlag {
	return &EntityPFlag{
		parser:   NewNodeParser(),
		typeName: "node",
		into:     into,
	}
}

// The returned pointer can be passed to cobra.Command.Flags().Var() to read in a target id from the
// user. into specifies where the parsed input shall be written to and also provides the default
// value.
func NewTargetPFlag(into *EntityId) *EntityPFlag {
	return &EntityPFlag{
		parser:   NewTargetParser(),
		typeName: "target",
		into:     into,
	}
}

// The returned pointer can be passed to cobra.Command.Flags().Var() to read in an buddy group id
// from the user. into specifies where the parsed input shall be written to and also provides the
// default value.
func NewBuddyGroupPFlag(into *EntityId) *EntityPFlag {
	return &EntityPFlag{
		parser:   NewBuddyGroupParser(),
		typeName: "buddyGroup",
		into:     into,
	}
}

// The returned pointer can be passed to cobra.Command.Flags().Var() to read in an storage pool id
// from the user. into specifies where the parsed input shall be written to and also provides the
// default value.
func NewStoragePoolPFlag(into *EntityId) *EntityPFlag {
	return &EntityPFlag{
		parser:   NewStoragePoolParser(),
		typeName: "storagePool",
		into:     into,
	}
}

// Implement pflag.Value
func (g *EntityPFlag) Type() string {
	return g.typeName
}

// Implement pflag.Value
func (g *EntityPFlag) String() string {
	if g.into != nil {
		return (*g.into).String()
	}
	return ""
}

// Implement pflag.value
func (g *EntityPFlag) Set(input string) error {
	r, err := g.parser.Parse(input)
	if err != nil {
		return err
	}
	*g.into = r
	return nil
}
