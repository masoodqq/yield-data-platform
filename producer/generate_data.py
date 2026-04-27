import json, time, random, os, uuid
from datetime import datetime, timezone

OUTPUT_DIR = "./wafer_outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

LOTS = ["LOT_A001", "LOT_A002", "LOT_B001", "LOT_B002"]
CELL_TYPES = ["ring_modulator", "mzi_switch", "grating_coupler", "phase_shifter"]
DEFECT_CODES = ["particle", "scratch", "void", "bridge", "open_circuit", None]
PROCESS_NODES = ["180nm", "130nm", "90nm"]

TESTS = [
    "insertion_loss",
    "extinction_ratio",
    "bandwidth",
    "dark_current",
    "responsivity"
]

def wafer_yield_profile():
    """Each wafer has a base yield between 60-95% with some variation per die."""
    return random.uniform(0.60, 0.95)

def generate_wafer_file():
    lot_id = random.choice(LOTS)
    wafer_id = f"W{random.randint(1, 25):02d}"
    process_node = random.choice(PROCESS_NODES)
    base_yield = wafer_yield_profile()
    filename = f"{OUTPUT_DIR}/wafer_{lot_id}_{wafer_id}_{uuid.uuid4().hex[:6]}.json"

    dies = []
    for x in range(10):
        for y in range(10):
            passed = random.random() < base_yield
            defect = random.choice(DEFECT_CODES) if not passed else None
            cell_type = random.choice(CELL_TYPES)

            test_results = {}
            for test in TESTS:
                if passed:
                    test_results[test] = round(random.uniform(-3.0, 3.0), 4)
                else:
                    test_results[test] = None

            die = {
                "lot_id":       lot_id,
                "wafer_id":     wafer_id,
                "process_node": process_node,
                "die_x":        x,
                "die_y":        y,
                "cell_type":    cell_type,
                "passed":       passed,
                "defect_code":  defect,
                "test_results": test_results,
                "tested_at":    datetime.now(timezone.utc).isoformat()
            }
            dies.append(die)

    with open(filename, "w") as f:
        for die in dies:
            f.write(json.dumps(die) + "\n")

    total = len(dies)
    passed = sum(1 for d in dies if d["passed"])
    print(f"Generated {filename} | {lot_id}/{wafer_id} | yield={passed/total:.1%} ({passed}/{total} dies)")

if __name__ == "__main__":
    print("Generating wafer test files every 15 seconds. Ctrl+C to stop.")
    while True:
        generate_wafer_file()
        time.sleep(15)