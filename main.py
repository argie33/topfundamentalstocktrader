import subprocess
import sys

def run_script(script_name):
    try:
        subprocess.check_call([sys.executable, script_name])
        print(f"Successfully ran {script_name}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to run {script_name}. Error: {e}")

def main():
    scripts = [
         "stocklistload.py",
         "balancesheet.py",
         "companyprofile.py",
         "incomestatement.py",
         "stocksfinancialgrowth.py",
         "stockskeymetrics.py",
         "stocksratios.py",
         "stockdatatablebuilder.py"
         "technicaldata.py",         
         "stockscreener.py",
         "loadexclusionlist.py",
         "loadwashsale.py"
    ]

    for script in scripts:
        run_script(script)

if __name__ == "__main__":
    main()
