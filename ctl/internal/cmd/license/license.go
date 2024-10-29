package license

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/spf13/cobra"
	licenseCmd "github.com/thinkparq/beegfs-go/v8/ctl/pkg/ctl/license"
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
	cmd.Flags().BoolVar(&cfg.Json, "json", false, "Output as JSON")

	return cmd
}

func runLicenseCmd(cmd *cobra.Command, cfg license_Config) error {
	license, err := licenseCmd.GetLicense(cmd.Context(), cfg.Reload)
	if err != nil {
		return err
	}
	// Even if there was no error, it is still possible that no license certificate data is available,
	// because the certificate was not loaded. We have to handle that case.
	if license.Result == pl.VerifyResult_VERIFY_ERROR {
		return errors.New(license.Message)
	}
	if cfg.Json {
		pretty, _ := json.MarshalIndent(license, "", "  ")
		fmt.Printf("%s\n", pretty)
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
	return nil
}
