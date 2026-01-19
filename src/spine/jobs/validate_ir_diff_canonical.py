import sys


def _import_impl():
    candidates = [
        ("US_TeaPlant.leaves.validate_ir_diff_canonical_cli", "main"),
        ("US_TeaPlant.leaves.ir_diff_validate_leaf_cli", "main"),
        ("US_TeaPlant.bridges.ir_diff_bridge", "validate"),
    ]
    last_err = None
    for mod_name, fn_name in candidates:
        try:
            mod = __import__(mod_name, fromlist=[fn_name])
            fn = getattr(mod, fn_name)
            return fn, mod_name, fn_name
        except Exception as e:
            last_err = e
    raise ImportError(f"No valid implementation found. Last error: {last_err}")


def main() -> int:
    try:
        impl_fn, mod_name, fn_name = _import_impl()
    except Exception as e:
        print(
            "[spine.jobs.validate_ir_diff_canonical] Implementation import failed.\n"
            f"Error: {e}",
            file=sys.stderr,
        )
        return 2

    try:
        return int(impl_fn() or 0)
    except Exception as e:
        print(
            "[spine.jobs.validate_ir_diff_canonical] Implementation raised an exception.\n"
            f"Entrypoint: {mod_name}:{fn_name}\n"
            f"Error: {e}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
