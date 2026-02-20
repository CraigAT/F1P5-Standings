import fastf1 as ff1
import pandas as pd
from datetime import datetime
import os

# 1. Configuration
current_year = 2026
excluded_teams = ["Mercedes", "Red Bull Racing", "Ferrari", "McLaren"]
race_points_map = {1: 25.0, 2: 18.0, 3: 15.0, 4: 12.0, 5: 10.0, 6: 8.0, 7: 6.0, 8: 4.0, 9: 2.0, 10: 1.0}
sprint_points_map = {1: 8.0, 2: 7.0, 3: 6.0, 4: 5.0, 5: 4.0, 6: 3.0, 7: 2.0, 8: 1.0}

def log_update(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("Logs/F1P5_Automation_Log.txt", "a") as f:
        f.write(f"[{timestamp}] {message}\n")

def get_standings_data(season_year):
    race_standings = []
    sprint_standings = []
    
    try:
        schedule = ff1.get_event_schedule(season_year, include_testing=False)
    except Exception as e:
        return [], [], f"Error fetching {season_year} schedule: {str(e)}"

    for _, event in schedule.iterrows():
        # Only pull races that have completed
        if event["EventDate"] > datetime.now():
            continue

        sessions_to_pull = {"Race": "R"}
        if event["EventFormat"] == "sprint_qualifying":
            sessions_to_pull["Sprint"] = "S"

        for session_type, session_code in sessions_to_pull.items():
            try:
                session = ff1.get_session(season_year, event["EventName"], session_code)
                session.load(laps=False, telemetry=False, weather=False, messages=False)
                
                if session.results.empty:
                    continue

                filtered_results = session.results[~session.results['TeamName'].isin(excluded_teams)]

                for _, driver_row in filtered_results.iterrows():
                    entry = {
                        "EventName": event["EventName"].replace("Grand Prix", "").strip(),
                        "RoundNumber": event["RoundNumber"],
                        "Driver": driver_row["Abbreviation"],
                        "DriverName": driver_row["FullName"],
                        "DriverNumber": driver_row["DriverNumber"],
                        "Team": driver_row["TeamName"],
                        "TeamColor": driver_row["TeamColor"],
                        "F1Class": str(driver_row["ClassifiedPosition"]),
                        "F1Order": int(driver_row["Position"]),
                        "F1Points": float(driver_row["Points"])
                    }
                    if session_type == "Race":
                        race_standings.append(entry)
                    else:
                        sprint_standings.append(entry)
            except:
                continue
                
    return race_standings, sprint_standings, "Success"

# 2. Main Execution with Fallback Logic
print(f"Checking for {current_year} data...")
races, sprints, status = get_standings_data(current_year)

# Fallback if no races have happened in the current year yet
if not races:
    fallback_year = current_year - 1
    print(f"No results found for {current_year}. Falling back to {fallback_year} for testing/validation...")
    races, sprints, status = get_standings_data(fallback_year)
    active_season = fallback_year
else:
    active_season = current_year

# 3. Processing Data Frames
race_df = pd.DataFrame(races)
sprint_df = pd.DataFrame(sprints)

def process_f1p5_safe(df, points_map):
    if df.empty: return df
    df["F1P5Order"] = df.groupby("RoundNumber")["F1Order"].rank(method="first").astype(int)
    is_num = df['F1Class'].str.isnumeric()
    df["F1P5Class"] = df["F1P5Order"].astype(str)
    df.loc[~is_num, "F1P5Class"] = df.loc[~is_num, "F1Class"]
    df["F1P5Points"] = 0.0
    df.loc[is_num, "F1P5Points"] = df.loc[is_num, "F1P5Order"].map(points_map).fillna(0.0)
    return df

race_df = process_f1p5_safe(race_df, race_points_map)
sprint_df = process_f1p5_safe(sprint_df, sprint_points_map)

# 4. Championship Generation
def get_countback(df, group_col, prefix):
    if df.empty: return pd.DataFrame()
    num_df = df[df['F1P5Class'].str.isnumeric()].copy()
    cb = pd.crosstab(num_df[group_col], num_df['F1P5Order'])
    cb.columns = [f'{prefix}{int(c)}' for c in cb.columns]
    return cb

all_results = pd.concat([race_df, sprint_df])

if not all_results.empty:
    # --- DRIVER STANDINGS ---
    driver_standings = all_results.groupby(['Driver', 'DriverName', 'DriverNumber', 'Team'])['F1P5Points'].sum().reset_index()
    d_race_cb = get_countback(race_df, 'Driver', "RP")
    d_sprint_cb = get_countback(sprint_df, 'Driver', "SP")
    driver_standings = driver_standings.merge(d_race_cb, on='Driver', how='left').merge(d_sprint_cb, on='Driver', how='left').fillna(0)
    
    cb_cols = [c for c in driver_standings.columns if c.startswith(('RP', 'SP'))]
    driver_standings[cb_cols] = driver_standings[cb_cols].astype(int)
    r_cols = sorted([c for c in driver_standings.columns if c.startswith('RP')], key=lambda x: int(x[2:]))
    driver_standings = driver_standings.sort_values(by=['F1P5Points'] + r_cols + ['DriverName'], ascending=[False] + [False]*len(r_cols) + [True])
    driver_standings.to_csv(f'Data/F1P5_{active_season}_Driver_Championship.csv', index=False)

    # --- TEAM STANDINGS ---
    team_standings = all_results.groupby('Team').agg({'F1P5Points': 'sum', 'TeamColor': 'first'}).reset_index()
    t_race_cb = get_countback(race_df, 'Team', "RP")
    t_sprint_cb = get_countback(sprint_df, 'Team', "SP")
    team_standings = team_standings.merge(t_race_cb, on='Team', how='left').merge(t_sprint_cb, on='Team', how='left').fillna(0)
    
    t_cb_cols = [c for c in team_standings.columns if c.startswith(('RP', 'SP'))]
    team_standings[t_cb_cols] = team_standings[t_cb_cols].astype(int)
    tr_cols = sorted([c for c in team_standings.columns if c.startswith('RP')], key=lambda x: int(x[2:]))
    team_standings = team_standings.sort_values(by=['F1P5Points'] + tr_cols + ['Team'], ascending=[False] + [False]*len(tr_cols) + [True])
    team_standings.to_csv(f'Data/F1P5_{active_season}_Team_Championship.csv', index=False)

    # --- RAW SESSION EXPORTS ---
    race_df.to_csv(f'Data/F1P5_{active_season}_Race_Results.csv', index=False)
    sprint_df.to_csv(f'Data/F1P5_{active_season}_Sprint_Results.csv', index=False)

    log_update(f"SUCCESS: Data exported for the {active_season} season.")
    print(f"Export complete using {active_season} data.")
else:
    log_update("WARNING: No data found in either current or previous season.")

    

