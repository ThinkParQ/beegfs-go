package license

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/dsnet/golib/unitconv"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	licenseCmd "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/license"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/node"
	pl "github.com/thinkparq/protobuf/go/license"
	"go.uber.org/zap"
)

type license_Config struct {
	Reload bool
	Json   bool
	Get    bool
}

var out_tpl = `%s
                  ##########                                  
                 #############                                
                 ###########################                  
                  #########      ######                  #####
                  #########      ######     #####      ###    
              #### ## #####      ######     ###### ## ##      
         ######### ## #####      ######     ###### ########   
      ############# # #####      ######     ###### ########## 
    ############### # #####      ######     ###### ###########
      ############# # #####      ######     ###### ###########
         ###########  #####      ######     ###### ########## 
              ######  #####      ######     ###### ######     
                      #####      ######     ######            
                       ####      ######     #####             
%s
%s
%s

Licensed to %s (%s, %s)
via Partner %s (%s, %s)

License period: %s - %s (%s)

Licensed machines: %s
Licensed capacity: %s

Licensed enterprise functionality:
`

// Creates new "license" command
func NewCmd() *cobra.Command {
	cfg := license_Config{}

	cmd := &cobra.Command{
		Use:   "license",
		Short: "Query license information",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfg.Get {
				return printGenerateLicenseHelp(cmd.Context(), "To obtain and install a new license:")
			}
			return runLicenseCmd(cmd, cfg)
		},
	}

	cmd.Flags().BoolVar(&cfg.Reload, "reload", false,
		"Reload and re-verify license certificate on the server.")
	cmd.Flags().BoolVar(&cfg.Get, "get", false,
		"Get and install a new license for this file system.")

	return cmd
}

func runLicenseCmd(cmd *cobra.Command, cfg license_Config) error {

	genLicHelpCalled := false
	// genLicHelpFunc is intended to be called using a defer so help text can be printed after other
	// output. It allows help text to be deferred multiple times based on independent conditionals
	// with only the last call actually printing any output.
	genLicHelpFunc := func(ctx context.Context, reason string) error {
		if !genLicHelpCalled {
			genLicHelpCalled = true
			return printGenerateLicenseHelp(ctx, reason)
		}
		return nil
	}

	license, err := licenseCmd.GetLicense(cmd.Context(), cfg.Reload)
	// Store the error (or nil if there is no error) in ret, so we can return from this function if
	// nothing else goes wrong along the way. This is important in case we reload a certificate that
	// fails verification. It will still be loaded by the mgmtd and not printing it would suggest it
	// wasn't loaded, but we also want to let the user know that there was an error.
	ret := err
	if err != nil {
		if cfg.Reload {
			// As noted above, we still want to print the certificate data in case of a failed
			// reload, so fetch it again. If we encounter another error, we return that. Otherwise,
			// we continue to the next check.
			license, err = licenseCmd.GetLicense(cmd.Context(), false)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	// Even if there was no error in either of the two `GetLicense()` calls, it is still possible
	// that no license certificate data is available, because the certificate was not loaded.
	// `license` could also be `nil` although that would be unexpected. We have to handle
	// these cases and return additional info about the original error in ret if available.
	// `errors.Join()` will gracefully handle the case of `ret == nil` as well.
	if license == nil {
		return errors.Join(ret, errors.New("license unexpectedly nil (this is probably a bug)"))
	}
	if license.Result == pl.VerifyResult_VERIFY_ERROR {
		err := genLicHelpFunc(cmd.Context(), "The system does not appear to have a valid community or enterprise license certificate loaded.")
		if err != nil {
			return errors.Join(ret, errors.New(license.Message), err)
		}
		return errors.Join(ret, errors.New(license.Message))
	}

	if viper.GetString(config.OutputKey) == config.OutputJSONPretty.String() {
		pretty, _ := json.MarshalIndent(license, "", "  ")
		fmt.Printf("%s\n", pretty)
	} else if viper.GetString(config.OutputKey) == config.OutputJSON.String() {
		json, _ := json.Marshal(license)
		fmt.Printf("%s\n", json)
	} else {
		var features []string
		var scopes []string
		var numservers string
		var capacityLimit string = "n/a"
		for _, f := range license.Data.DnsNames {
			if after, ok := strings.CutPrefix(f, licenseCmd.PrefixNumServers); ok {
				numservers = after
			} else if after, ok := strings.CutPrefix(f, licenseCmd.PrefixCapacity); ok {
				capacityLimit = after
				cl, err := strconv.ParseUint(capacityLimit, 10, 64)
				if err != nil {
					log, _ := config.GetLogger()
					log.Debug("unable to convert capacity limit to an integer, ignoring and printing value as is", zap.Error(err))
				} else if !viper.GetBool(config.RawKey) {
					// Note the cast of a uint64 to a float64 will lose precision with large numbers.
					capacityLimit = fmt.Sprintf("%sB", unitconv.FormatPrefix(float64(cl), unitconv.IEC, 1))
				}
			} else if after, ok := strings.CutPrefix(f, licenseCmd.PrefixScope); ok {
				scopes = append(scopes, after)
			} else {
				features = append(features, fmt.Sprintf("  - %s", f))
			}
		}

		var header string
		// A license can have multiple scopes, so we order them by "importance" here. Scopes will
		// be listed below again.
		if slices.Contains(scopes, "nfr") {
			header = fmt.Sprintf("NOT FOR RESALE License Certificate %s", license.Data.CommonName)
		} else if slices.Contains(scopes, "beeond") {
			header = fmt.Sprintf("BeeOND License Certificate %s", license.Data.CommonName)
		} else if license.Data.Type == pl.CertType_CERT_TYPE_CUSTOMER {
			header = fmt.Sprintf("BeeGFS Enterprise License Certificate %s", license.Data.CommonName)
		} else if license.Data.Type == pl.CertType_CERT_TYPE_TEMPORARY {
			header = fmt.Sprintf("BeeGFS Temporary License Certificate %s", license.Data.CommonName)
			defer genLicHelpFunc(cmd.Context(), "\nWARNING: This system has a temporary license installed.")
		} else if license.Data.Type == pl.CertType_CERT_TYPE_COMMUNITY {
			header = fmt.Sprintf("BeeGFS Community License Certificate %s", license.Data.CommonName)
		} else {
			header = fmt.Sprintf("Unknown BeeGFS License Certificate %s", license.Data.CommonName)
		}

		var color string
		switch license.Data.Type {
		case pl.CertType_CERT_TYPE_CUSTOMER:
			color = "\033[32m" // Green if license is valid and customer license
		case pl.CertType_CERT_TYPE_TEMPORARY:
			color = "\033[33m" // Yellow if license is valid and temporary license
		default:
			color = "\033[34m" // Blue for all other license types including community licenses
		}

		// Check if any license conditions are violated and that the license is valid.
		if err := licenseCmd.CheckIfOverStorageCapacityLimit(cmd.Context(), capacityLimit); err != nil {
			color = "\033[31m" // Red if there is a license violation.
			defer cmdfmt.Printf("\nWARNING: The %s.\n", err)
		}
		if license.Result == pl.VerifyResult_VERIFY_INVALID {
			color = "\033[31m" // Red if license is invalid
			defer genLicHelpFunc(cmd.Context(), fmt.Sprintf("\nWARNING: The installed license is not valid and should be updated to avoid disruptions. \nReason: %s.", license.Message))
		}

		// BeeGFS enterprise licenses are typically issued based on contract dates in GMT. The
		// certificate validity includes a grace period so the license is usable worldwide from
		// 00:00 UTC-14 to 23:59 UTC+12. Therefore, ValidFrom is 14 hours before the contract start
		// date and ValidUntil is 12 hours after the contract end date. To display the contract
		// dates, we shift the validity period back to the original GMT dates.
		//
		// IMPORTANT: IF this ever changes also update GetTimeToExpiration(),
		validFrom := license.Data.ValidFrom.AsTime().Add(14 * time.Hour)
		validUntil := license.Data.ValidUntil.AsTime().Add(-12 * time.Hour)
		_, daysLeftMessage := licenseCmd.GetTimeToExpiration(license)
		fmt.Printf(out_tpl,
			color,     // Color the bee
			"\033[0m", // Reset font color
			header,
			strings.Repeat("=", len(header)),
			license.Data.Organization,
			license.Data.Locality,
			license.Data.Country,
			license.Data.ParentData.Organization,
			license.Data.ParentData.Locality,
			license.Data.ParentData.Country,
			validFrom.Format("2006-01-02 MST"),
			validUntil.Format("2006-01-02 MST"),
			daysLeftMessage,
			numservers,
			capacityLimit)
		for _, f := range features {
			fmt.Println(f)
		}
		if len(features) == 0 {
			fmt.Println("  - None")
		}
		if len(scopes) > 0 {
			fmt.Printf("\nLicense scopes and limitations:\n")
			if slices.Contains(scopes, "nfr") {
				fmt.Println("  - This license is NOT FOR RESALE and may be used for testing purposes only")
			}
			if slices.Contains(scopes, "beeond") {
				fmt.Println("  - This is a license for BeeOND (BeeGFS On Demand) file systems")
			}
		}
	}
	return ret
}

// printGenerateLicenseHelp should be used anytime the user needs instructions how to obtain licenses.
// Note in runLicenseCmd this should not be called directly and instead genLicHelpFunc used instead.
func printGenerateLicenseHelp(ctx context.Context, reason string) error {
	url, err := generateLicenseURL(ctx)
	if err != nil {
		return err
	}

	fmt.Printf(`%s

If you already have a community or enterprise license place it at /etc/beegfs/license.pem on the server running the management service then run: beegfs license --reload

If you need a license:

* Enterprise licenses are available by contacting %s.
* Community licenses are available free of charge, however we require you to share some information to help us improve BeeGFS.

A community license for this file system can be generated by pasting the following URL into your web browser:

%s

If you have any questions please reach out to %s.

`, reason, licenseCmd.ContactEmail, url.String(), licenseCmd.ContactEmail)
	return nil
}

func generateLicenseURL(ctx context.Context) (url.URL, error) {
	getArgs, err := assembleGetArgs(ctx)
	if err != nil {
		return url.URL{}, fmt.Errorf("unable to generate a license url for this system: %s (please contact support)", err)
	}
	var url url.URL
	url.RawQuery = getArgs
	url.Scheme = "https"
	url.Host = "beegfs.io"
	url.Path = "license/"
	return url, nil
}

func assembleGetArgs(ctx context.Context) (string, error) {

	mgmtd, err := config.ManagementClient()
	if err != nil {
		return "", fmt.Errorf("unable to connect to management: %w", err)
	}

	fsUUID, err := mgmtd.GetFsUUID(ctx)
	if err != nil {
		return "", err
	}

	allNodes, err := node.GetNodes(ctx, node.GetNodes_Config{
		WithNics: true,
	})

	if err != nil {
		return "", err
	}

	totalStorageCapacity, err := licenseCmd.TotalStorageCapacity(ctx)
	if err != nil {
		return "", err
	}

	var numMeta int
	var numStorage int
	var netProto = "tcp"
	for _, node := range allNodes {
		switch node.Node.Id.NodeType {
		case beegfs.Meta:
			numMeta++
		case beegfs.Storage:
			numStorage++
		}
		for _, nic := range node.Nics {
			if nic.Nic.Type == beegfs.Rdma {
				netProto = "rdma"
			}
		}
	}
	getArgs, err := util.URLEncodeSignMap(map[string]string{
		"capacity":    strconv.FormatUint(totalStorageCapacity, 10),
		"num_meta":    strconv.Itoa(numMeta),
		"num_storage": strconv.Itoa(numStorage),
		"net_proto":   netProto,
		"uuid":        fsUUID,
	}, "uuid")
	if err != nil {
		return "", err
	}
	return getArgs, nil
}
