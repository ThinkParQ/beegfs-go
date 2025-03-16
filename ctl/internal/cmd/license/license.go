package license

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	licenseCmd "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/license"
	pl "github.com/thinkparq/protobuf/go/license"
)

type license_Config struct {
	Reload bool
	Json   bool
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

Number of servers: %s

Licensed functionality:
`

// Creates new "license" command
func NewCmd() *cobra.Command {
	cfg := license_Config{}

	cmd := &cobra.Command{
		Use:   "license",
		Short: "Query license information",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLicenseCmd(cmd, cfg)
		},
	}

	cmd.Flags().BoolVar(&cfg.Reload, "reload", false,
		"Reload and re-verify license certificate on the server")

	return cmd
}

func runLicenseCmd(cmd *cobra.Command, cfg license_Config) error {
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
		return errors.Join(ret, errors.New("license unexpectly nil (this is probably a bug)"))
	}
	if license.Result == pl.VerifyResult_VERIFY_ERROR {
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
		var numservers string
		for _, f := range license.Data.DnsNames {
			if strings.HasPrefix(f, "io.beegfs.numservers.") {
				numservers = strings.TrimPrefix(f, "io.beegfs.numservers.")
			} else {
				features = append(features, fmt.Sprintf("  - %s", f))
			}
		}
		var color string
		switch license.Data.Type {
		case pl.CertType_CERT_TYPE_CUSTOMER:
			if license.Result == pl.VerifyResult_VERIFY_VALID {
				color = "\033[32m" // Green if license is valid and custmer license
			}
		case pl.CertType_CERT_TYPE_TEMPORARY:
			if license.Result == pl.VerifyResult_VERIFY_VALID {
				color = "\033[33m" // Yellow if license is valid and temporary license
			}
		}
		if license.Result == pl.VerifyResult_VERIFY_INVALID {
			color = "\033[31m" // Red if license is invalid
		}
		header := fmt.Sprintf("BeeGFS Customer License Certificate %s", license.Data.CommonName)
		// All BeeGFS license certificates are valid from 00:00 UTC-14 until 23:59 UTC+12, so we
		// adjust for that in the output
		validFrom := license.Data.ValidFrom.AsTime().Add(14 * time.Hour)
		validUntil := license.Data.ValidUntil.AsTime().Add(-12 * time.Hour)
		var daysLeftMessage string
		daysLeft := int(math.Floor(time.Until(validUntil).Hours() / 24))
		if daysLeft < -1 {
			daysLeftMessage = fmt.Sprintf("expired %d days ago", -1*daysLeft)
		} else if daysLeft == -1 {
			daysLeftMessage = "expired yesterday"
		} else if daysLeft == 0 {
			daysLeftMessage = "expires end of day today"
		} else if daysLeft == 1 {
			daysLeftMessage = "expires tomorrow"
		} else if daysLeft > 0 {
			daysLeftMessage = fmt.Sprintf("expires in %d days", daysLeft)
		}
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
			validFrom.Format("2006-01-02"),
			validUntil.Format("2006-01-02"),
			daysLeftMessage,
			numservers)
		for _, f := range features {
			fmt.Println(f)
		}
	}
	return ret
}
