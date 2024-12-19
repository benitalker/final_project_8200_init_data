from pathlib import Path
from flask import Flask, render_template
from app.rout.group1_questions_routs import stats_blueprint
from flask_cors import CORS

base_path = Path(__file__).resolve().parent.parent / "templates"
app = Flask(__name__, template_folder=base_path)
CORS(app)

# Define question-to-action mappings with descriptions
QUESTION_CONFIG = {
    "deadliest_attacks": {
        "type": "chart",
        "title": "Deadliest Attack Types",
        "description": "Analysis of the most lethal types of attacks based on casualties",
        "params": ["top_n"]
    },
    "casualties_by_region": {
        "type": "map",
        "title": "Casualties by Region",
        "description": "Geographic distribution of casualties across regions",
        "params": ["top_n"]
    },
    "top_casualty_groups": {
        "type": "chart",
        "title": "Top Casualty Groups",
        "description": "Most lethal terrorist groups based on total casualties",
        "params": []
    },
    "attack_target_correlation": {
        "type": "chart",
        "title": "Attack-Target Correlation",
        "description": "Correlation analysis between attack types and target types",
        "params": []
    },
    "attack_trends": {
        "type": "chart",
        "title": "Attack Trends",
        "description": "Annual and monthly trends of terrorist attacks",
        "params": ["year"]
    },
    "attack_change_by_region": {
        "type": "chart",
        "title": "Attack Change by Region",
        "description": "Analysis of changes in attack frequency by region",
        "params": ["top_n"]
    },
    "terror_heatmap": {
        "type": "map",
        "title": "Terror Heatmap",
        "description": "Heat map visualization of terror incidents",
        "params": ["period", "region"]
    },
    "active_groups_heatmap": {
        "type": "map",
        "title": "Active Groups Heatmap",
        "description": "Geographic distribution of active terrorist groups",
        "params": ["region"]
    },
    "perpetrators_casualties_correlation": {
        "type": "chart",
        "title": "Perpetrators-Casualties Correlation",
        "description": "Correlation between number of perpetrators and casualties",
        "params": []
    },
    "events_casualties_correlation": {
        "type": "chart",
        "title": "Events-Casualties Correlation",
        "description": "Correlation between number of events and casualties",
        "params": ["region"]
    }
}

# Register the blueprint
app.register_blueprint(stats_blueprint, url_prefix='/stats')

@app.route("/")
def home():
    return render_template(
        "index.html",
        questions=QUESTION_CONFIG
    )

if __name__ == "__main__":
    app.run(debug=True)