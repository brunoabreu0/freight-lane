"""CLI entry point for the banking-system monolith.

Each subcommand starts a different component of the ATM withdrawal pipeline.
All components share the same Docker image but run different code paths.

Usage:
    banking-system atm               # Start the ATM (producer + consumer)
    banking-system fraud-detection    # Start Fraud Detection
    banking-system account-balance    # Start Account Balance
    banking-system ledger             # Start Transaction Ledger
    banking-system analytics          # Start Financial Analytics
    banking-system notifications      # Start Push Notifications
"""

from __future__ import annotations

import sys


COMPONENTS = {
    "atm": ("banking_system.atm", "run"),
    "create-atm-request": ("banking_system.atm", "trigger_request_cli"),
    "fraud-detection": ("banking_system.fraud_detection", "run"),
    "account-balance": ("banking_system.account_balance", "run"),
    "ledger": ("banking_system.downstream", "run_ledger"),
    "analytics": ("banking_system.downstream", "run_analytics"),
    "notifications": ("banking_system.downstream", "run_notifications"),
}


def main() -> None:
    """Parse the subcommand and dispatch to the appropriate component."""
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("Usage: banking-system <component>")
        print()
        print("Available components:")
        for name in COMPONENTS:
            print(f"  {name}")
        sys.exit(0 if len(sys.argv) >= 2 else 1)

    component = sys.argv[1]
    if component not in COMPONENTS:
        print(f"Error: Unknown component '{component}'")
        print(f"Available: {', '.join(COMPONENTS)}")
        sys.exit(1)

    module_path, func_name = COMPONENTS[component]

    # Dynamic import to avoid loading all components on every invocation
    import importlib
    module = importlib.import_module(module_path)
    func = getattr(module, func_name)
    func()


if __name__ == "__main__":
    main()
