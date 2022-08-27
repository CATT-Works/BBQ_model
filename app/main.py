from flask import Flask, request, redirect
from modelzoo import ModelZoo
from os import linesep

modelzoo = ModelZoo()


app = Flask(__name__)

@app.route('/')
def hello():
    return """
    Avaliable endpoints:<br>
        - /now?direction={East/West}&horizon={5,10,15,20,25,30,all}<br>
        - /East<br>
        - /West<br>
    """
    name = request.args.get('name', 'World')
    return f'Hello {escape(name)}!'


@app.route('/East/')
def EastModel():
    return redirect('/now?direction=East')

@app.route('/West/')
def WestModel():
    return redirect('/now?direction=West')

@app.route('/now/')
def estimateNow():
    direction = request.args.get('direction')
    horizon = request.args.get('horizon')
    outputformat = request.args.get('outputformat')

    feasible_directions = ['East', 'West'] 
    feasible_horizons = ['5', '10', '15', '20', '25', '30', 'all']

    if direction is None:
        return "Error: direction not specified."
    
    direction = direction.capitalize()

    if horizon is None:
        horizon = 'all'
        
    if horizon not in feasible_horizons:
        return f"Error: horizon must be one of {feasible_horizons}. Instead, it is {horizon}."
        
    resdf = modelzoo.estimate_now(direction, horizon)
    
    if outputformat == 'csv':
        return resdf.to_csv(index=False, line_terminator=modelzoo.LINETERMINATOR)
    else:
        return resdf.to_json()