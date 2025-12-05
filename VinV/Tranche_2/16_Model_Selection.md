from pathlib import Path

Path("Tranche_2").mkdir(exist_ok=True)

Path("Tranche_2/16_Model_Selection.md").write_text("""
# VinV Tranche 2 – Model Selection

Selected model: Gradient Boosting Regressor

Rationale:
- Linear baseline confirms weak linear separability
- Tree models capture nonlinear cross-sectional structure
- GB chosen over RF due to smoother generalization

Status:
✔ Tranche 2 complete
""".strip())
