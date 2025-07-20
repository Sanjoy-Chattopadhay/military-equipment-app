import pandas as pd
from datetime import datetime

class EquipmentModels:
    @staticmethod
    def calculate_critical_fault_respect(row):
        """Calculate reliability based on critical faults"""
        count = row.get('totalcriticalfaultcount', 0)
        if pd.isna(count):
            return 'Unknown'
        try:
            count = int(count)
            if count <= 2:
                return 'Reliable'
            elif 3 <= count <= 5:
                return 'Partially Reliable'
            else:
                return 'Not Reliable'
        except:
            return 'Invalid'

    @staticmethod
    def calculate_vintage_respect(row):
        """Calculate reliability based on vintage year"""
        year = row.get('year')
        if pd.isna(year):
            return 'Unknown'
        try:
            year = int(year)
            if year <= 2009:
                return 'Not Reliable'
            elif year < 2015:
                return 'Partially Reliable'
            return 'Reliable'
        except:
            return 'Invalid'

    @staticmethod
    def calculate_km_respect(row):
        """Calculate reliability based on kilometers"""
        km = row.get('inkm', 0)
        if pd.isna(km):
            return 'Unknown'
        try:
            if km <= 40000:
                return 'Reliable'
            elif km <= 90000:
                return 'Partially Reliable'
            return 'Not Reliable'
        except:
            return 'Invalid'

    @staticmethod
    def calculate_priority(row):
        """Calculate priority based on cumulative scoring"""
        score_map = {
            'Reliable': 3,
            'Partially Reliable': 2,
            'Not Reliable': 1
        }

        cumulative_score = 0
        cumulative_score += score_map.get(row.get('respecttovintage'), 0)
        cumulative_score += score_map.get(row.get('respecttodistance'), 0)
        cumulative_score += score_map.get(row.get('respecttocriticalfaults'), 0)

        if cumulative_score == 9:
            return 'P1'
        elif cumulative_score == 8:
            return 'P2'
        elif cumulative_score == 7:
            return 'P3'
        elif cumulative_score == 6:
            return 'P4'
        else:
            return 'P5'

    @staticmethod
    def get_upcoming_maintenance_tasks(current_km, input_km):
        """Calculate upcoming maintenance tasks"""
        service_tasks = {
            5000: [
                "Change engine oil and oil filter",
                "Replace fuel filter",
                "Inspect and adjust brakes"
            ],
            10000: [
                "Check gearbox and differential oil",
                "Inspect and adjust clutch",
                "Inspect suspension system"
            ],
            20000: [
                "Engine tune-up",
                "Clean fuel tank and lines"
            ]
        }

        if pd.isna(current_km) or pd.isna(input_km):
            return "🚫 Insufficient data to calculate upcoming maintenance."

        try:
            current_km = int(current_km)
            input_km = int(input_km)
            future_km = current_km + input_km

            output_lines = [f"### 🔧 Maintenance due within next **{input_km} km**"]
            any_task_shown = False

            for interval, tasks in service_tasks.items():
                next_due_km = ((current_km // interval) + 1) * interval
                km_remaining = next_due_km - current_km

                if next_due_km <= future_km:
                    any_task_shown = True
                    output_lines.append(f"**After {km_remaining} km (at {next_due_km:,} km)** → Perform:")
                    for task in tasks:
                        output_lines.append(f"- {task} (Every {interval:,} km)")
                    output_lines.append("")

            if not any_task_shown:
                return f"✅ No scheduled maintenance within the next **{input_km} km**."

            return "\n".join(output_lines)

        except Exception as e:
            return f"⚠️ Error calculating maintenance tasks: {e}"
