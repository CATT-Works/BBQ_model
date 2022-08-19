# BBQ_model
First version of the Bay Bridge Queue prediction code.

#### Creating container

To create a container you should:
- set USERNAME and PASSWORD variables in ./app/config.yaml
- build the container with: docker build -t bbq-pred:1.0 .


#### Running container
It should be ready for running, for example:
docker run -p 5000:5000 bbq-pred:1.0

You can also overwrite the credentials like this:
docker run -p 5000:5000 -e USERNAME=your_username -e PASSWORD=your_password bbq-pred:1.0


#### Using the container
For now, there is one endpoint available
/now?direction={East/West}&horizon={5,10,15,20,25,30,all}
- direction: estimates for eastbound/westbound direction
- horizon: time horizon for estimates (5 - 30 minutes)

Examples:
- http://localhost:5000/now/?direction=West&horizon=15: Estimates speed ratios 15 minutes ahead in westbound direction.
- http://localhost:5000/now/?direction=East: Estimates speed ratios 5-30 minutes ahead in eastbound direction (default horizon is `all`)
- http://localhost:5000/East: link, same command as above


# TODO
- Discuss output format
- Review models with Sara
- Add endpoint that estimates outputs for various bridge configurations
- Add endpoint that estimates historical outputs (requires addional functionality in baybridge speed endpoint)
- Add endpoint with shap-based explainability (to be discussed)







