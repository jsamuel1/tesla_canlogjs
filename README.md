# tesla_canlogjs
Capture CAN messages on a RPi and upload to AWS for processing and analysis

# Setup Instructions
## Car Setup
### Bill of Materials
 * Raspberry Pi 4
 * Tesla CAN harness adaptor
 * White Panda (from comma.ai) CAN interface
 * USB Cables
### Software Requirements
 * Base image of TeslaUSB installed on the Raspberry Pi  (or equiv compatible filesystem layout -- assumes a /backingstore folder exists)
 * An AWS account with an S3 bucket for storing data
 * Setup the Raspberry Pi to connect to your home wifi before installing in the car

### Installation in Car
 * For a Tesla Model 3, install a 3rd party CAN wiring harness below centre console, following the instructions for the hardness carefully. **This may void your warranty** 
 * Install White Panda (or equiv compatible CAN adaptor) to the harness and connect via USB to the Raspberry Pi.   Note - don't use power from the CAN, as the Raspberry Pi doesn't deal with slightly out-of-spec voltage from the white panda - use a Panda Paw to disconnect the power.
 * Install Raspberry Pi power from centre console front USB (if storing dashcam footage via TeslaUSB -- otherwise, from any USB port in the car).
 * Download the rpi folder from this repo to your Raspberry Pi
 * <temporary step>modify can_upload.py with your AWS access keys and bucket name
 * run install.sh as sudo, in order to install the capture and upload services
 
 ## Cloud Setup
 * Ensure you have valid AWS access credentials in your environment
 *  Create/identify a parent Route53 domain and domainName for this service
 *  Create a `cdk.context.json` file in the cdk folder with context of route53 such as:
 ```{
  "domainName": "canlogjs.myspecialdomain.wtf",
  "hostedZoneId": "Z0000000SMC0GXXSF00X",
  "zoneName": "specialdomain.wtf"
}
```
 * `cdk cdk`
 * `cdk bootstrap`
 * `cdk deploy`

