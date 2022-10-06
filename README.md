# BBQ_model
First version of the Bay Bridge Queue prediction code.

### Creating container

To create a container you should:
- set USERNAME and PASSWORD variables in ./app/config.yaml
- build the container with: docker build -t bbq-pred:1.0 .


### Using the container

Container may be run from the command line.

Parameters:
  - `-h, --help`: shows help message.
  - `-t, --token TOKEN`: Baybridge endpoints token. If you create a token.txt file, you may leave this parameter empty
  - `-s, --speedendpoint SPEEDENDPOINT`: Speed endpoint. Default: https://baybridge.ritis.org/speed/recent/
  - `-b, --bbendpoint BBENDPOINT`:  Bay Bridge status endpoint. No need to define it, if you are using `-f all`. Default: https://baybridge.ritis.org/status/
  - `-d, --direction {East,West}`: Traffic direction
  - `-f, --forecasthorizon {all,5,10,15,20,25,30}`: Horizon of estimates. Default: all
  - `-o, --outputformat {json,csv,df}`: Output format. Default: json

Examples:
- `docker run --rm bbq-pred:1.0 -d West` - Generates all westbound estimates. Works only if you created token.txt file.

- `docker run --rm bbq-pred:1.0 -d East -t YOUR_TOKEN -f 30` - Generates 30 min eastbound estimates.

