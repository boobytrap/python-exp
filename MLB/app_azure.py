from flask import Flask, render_template, request
from MultiTeamPlayers import MultiTeamPlayers

app = Flask(__name__)

# Sample list of objects
items = []
with open("mlb_teams.txt", "r") as f:
        for line in f:
            items.append(line.strip())

@app.route('/')
def index():
    return render_template('index.html', items=items)

@app.route('/calculate', methods=['POST'])

def calculate():
    #selected_item = request.form['item']
    team1 = request.form['item']
    team2 = request.form['item2']
    # Perform some calculation with the selected item
    if team1 == team2:
         result = f"Please choose two different teams"
         return render_template('index.html', items=items, result=result)
    
    result = f"Players who played for the {team1} and the {team2}"
    d = multi_team_players.getTwoTeamPlayers([team1, team2])  
    return render_template('index.html', items=items, items2=items, team1=team1, team2=team2, selected_team1=team1, selected_team2=team2, result=result, data =d)

if __name__ == '__main__':
    multi_team_players  = MultiTeamPlayers()
    app.run(debug=True)
    multi_team_players.stopSpark()

