// bflag provides interfaces and types for wrapping arguments passed to external BeeGFS command-line
// applications using exec.Command() with Cobra and pflags. It applies a consistent set of rules
// when translating pflags to flags used with some other CLI application.
//
// Typical use looks like:
//
//			var bflagSet *bflag.FlagSet
//			cmd := &cobra.Command{
//			Use: "some-command",
//			RunE: func(cmd *cobra.Command, args []string) error {
//			    fmt.Println(bflagSet.WrappedArgs())
//			    return nil
//			    },
//			}
//			myFlags := []bflag.Wrapper{
//			  bflag.Flag("string-flag", "s", "A string flag!", "-s", "default value")
//			  bflag.Flag("int-flag", "i", "An int flag!", "-i", 0)
//			  bflag.Flag("bool-flag", "b", "A bool flag!", "-b", false)
//		      bflag.GlobalFlag(config.NumWorkersKey, "-t"),
//			}
//			bflagSet = bflag.NewFlagSetWithBindings(myFlags, cmd)
//	     // Then flags can be marked as required/mutually exclusive/etc.:
//	     cmd.MarkFlagRequired("string-flag")
//
// The resulting bflagSet.WrappedArgs() would look like:
//
//	[]string{"-s", "default value", "-i", 0, "-t", 4}
package bflag

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// FlagSet can be optionally used to work with Wrappers instead of directly using them.
type FlagSet struct {
	flags []FlagWrapper
	cmd   *cobra.Command
}

// NewFlagSet accepts a slice of Wrappers and a command to bind the flags to. Once flags are bound
// they cannot be updated. Typically this should happen right after declaring the command.
func NewFlagSet(flags []FlagWrapper, cmd *cobra.Command) *FlagSet {
	for _, flag := range flags {
		flag.bindPFlag(cmd)
	}
	return &FlagSet{
		flags: flags,
		cmd:   cmd,
	}
}

// WrappedArgs returns a slice of args intended for use with exec.Command(). It can be called
// multiple times if needed to return the wrapped args. Refer to the rules on the Flag() function
// for how args are handled for different types.
func (f *FlagSet) WrappedArgs() []string {
	// Length of the target slice is unknown since some flags may not be set.
	args := make([]string, 0)
	for _, flag := range f.flags {
		arg := flag.toWrappedArg()
		if arg != nil {
			args = append(args, arg...)
		}
	}
	return args
}

// FlagWrapper is an interface for wrapping flags exposed to a user using a Cobra command that are
// passed through to some external BeeGFS CLI tool.
type FlagWrapper interface {
	// bindPFlag binds this flag to a cobra command.
	bindPFlag(cmd *cobra.Command)
	// toWrappedArg converts this flag to string arguments for the wrapped application.
	toWrappedArg() []string
}

// baseFlag contains fields found in all flag types.
type baseFlag struct {
	Name       string
	Shorthand  string
	Usage      string
	WrappedArg string
}

type flagValue interface {
	string | int | bool
}

type flagConfig struct {
	withEquals bool
}

type flagOpt (func(*flagConfig))

// WithEquals() causes string and integer flags to be passed to the wrapped application as
// `WrappedArg=Value`. This is helpful if the wrapped application requires an equal sign or if you
// need to pass a flag like `--size=-1G` that if passed like `--size -1G` may interpret -1G as a
// flag instead of a value.
func WithEquals() flagOpt {
	return func(fc *flagConfig) {
		fc.withEquals = true
	}
}

// Flag is a generic function that creates and returns wrapped flags inferring the flag type from
// the default value. Note the following rules when specifying the defaultValue if no wrapped arg
// should be returned if the flag is not specified by the user:
//
//   - Strings: Omitted if set to an empty string ("").
//   - Integers: Always included.
//   - Booleans: Omitted if set to false.
//
// These rules are also applied to user provided input. As a special case boolean flags are always
// only returned if the associated value is true, and then they will be returned as simply the
// configured WrappedArg, for example "-f" NOT "-f=true".
func Flag[T flagValue](name, shorthand, usage, wrappedArg string, defaultValue T, opts ...flagOpt) FlagWrapper {

	cfg := &flagConfig{
		withEquals: false,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	baseFlag := baseFlag{
		Name:       name,
		Shorthand:  shorthand,
		Usage:      usage,
		WrappedArg: wrappedArg,
	}
	// Convert the defaultValue of type T to any then perform a type switch so we can work directly
	// with the underlying type. This is required to assign the generic default value to the default
	// field of a concrete type.
	switch dVal := any(defaultValue).(type) {
	case string:
		return &stringFlag{
			baseFlag: baseFlag,
			Default:  dVal,
			Value:    new(string),
			config:   *cfg,
		}
	case int:
		return &intFlag{
			baseFlag: baseFlag,
			Default:  dVal,
			Value:    new(int),
			config:   *cfg,
		}
	case bool:
		return &boolFlag{
			baseFlag: baseFlag,
			Default:  dVal,
			Value:    new(bool),
		}
	default:
		// A panic will only happen if the FlagValue interface was expanded without actually adding
		// support here for the new flag type. This type of bug should be caught in development
		// after defining an invalid flag so panicking is acceptable.
		panic(fmt.Sprintf("unsupported flag type: %T", defaultValue))
	}
}

type stringFlag struct {
	baseFlag
	Default string
	Value   *string
	config  flagConfig
}

func (f *stringFlag) bindPFlag(cmd *cobra.Command) {
	if f.Shorthand != "" {
		cmd.Flags().StringVarP(f.Value, f.Name, f.Shorthand, f.Default, f.Usage)
	} else {
		cmd.Flags().StringVar(f.Value, f.Name, f.Default, f.Usage)
	}
}

func (f *stringFlag) toWrappedArg() []string {
	if f.Value != nil && *f.Value != "" {
		if f.config.withEquals {
			return []string{fmt.Sprintf("%s=%s", f.WrappedArg, *f.Value)}
		}
		return []string{f.WrappedArg, *f.Value}
	}
	return nil
}

type intFlag struct {
	baseFlag
	Default int
	Value   *int
	config  flagConfig
}

func (f *intFlag) bindPFlag(cmd *cobra.Command) {
	if f.Shorthand != "" {
		cmd.Flags().IntVarP(f.Value, f.Name, f.Shorthand, f.Default, f.Usage)
	} else {
		cmd.Flags().IntVar(f.Value, f.Name, f.Default, f.Usage)
	}
}

func (f *intFlag) toWrappedArg() []string {
	if f.Value != nil {
		if f.config.withEquals {
			return []string{fmt.Sprintf("%s=%d", f.WrappedArg, *f.Value)}
		}
		return []string{f.WrappedArg, strconv.Itoa(*f.Value)}
	}
	return nil
}

type boolFlag struct {
	baseFlag
	Default bool
	Value   *bool
}

func (f *boolFlag) bindPFlag(cmd *cobra.Command) {
	if f.Shorthand != "" {
		cmd.Flags().BoolVarP(f.Value, f.Name, f.Shorthand, f.Default, f.Usage)
	} else {
		cmd.Flags().BoolVar(f.Value, f.Name, f.Default, f.Usage)
	}
}

func (f *boolFlag) toWrappedArg() []string {
	if f.Value != nil && *f.Value {
		return []string{f.WrappedArg}
	}
	return nil
}

// GlobalFlag is a special wrapper flag that allows wrapping global flags. Calling BindPFlag is a
// no-op and ToWrappedArg() uses whatever value is set in Viper for the specified name, which may
// either be the default or a value specified by the user. An empty string is returned if there is
// no global configuration flag for name, or if the string representation of value is empty.
//
// Note as a special case boolean flags are only returned if the associated value is true, and then
// they will be returned as simply the configured WrappedArg, for example "-f" NOT "-f=true".
func GlobalFlag(name, wrappedArg string) FlagWrapper {
	return &globalFlag{
		baseFlag: baseFlag{
			Name:       name,
			WrappedArg: wrappedArg,
		},
	}
}

type globalFlag struct {
	baseFlag
}

func (f *globalFlag) bindPFlag(cmd *cobra.Command) {
	// BindPFlag is a no-op for global flags since they are bound elsewhere.
}

func (f *globalFlag) toWrappedArg() []string {
	arg := viper.Get(f.Name)
	switch arg.(type) {
	case bool:
		if viper.GetBool(f.Name) {
			return []string{f.WrappedArg}
		}
		return nil
	default:
		s := viper.GetString(f.Name)
		if s != "" {
			return []string{f.WrappedArg, viper.GetString(f.Name)}
		}
		return nil
	}
}
