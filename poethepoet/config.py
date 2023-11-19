import json
from pathlib import Path

try:
    import tomllib as tomli
except ImportError:
    import tomli  # type: ignore[no-redef]

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

from .exceptions import PoeException
from .options import NoValue, PoeOptions

if TYPE_CHECKING:
    pass


class ConfigPartition:
    ConfigOptions: Type[PoeOptions]
    options: PoeOptions
    full_config: Mapping[str, Any]
    poe_options: Mapping[str, Any]
    path: Path

    def __init__(
        self, full_config: Mapping[str, Any], cwd: Path, path: Optional[Path] = None
    ):
        self.poe_options: Mapping[str, Any] = (
            full_config["tool"]["poe"]
            if "tool" in full_config
            else full_config["tool.poe"]
        )
        self.options = self.ConfigOptions(self.poe_options)
        self.full_config = full_config
        self.path = path

    def get(self, key: str, default: Any = NoValue):
        return self.options.get(key, default)

    def is_option_type_valid(self, key: str):
        value = self.options.get(key, None)
        expected_type = self.options.type_of(key)
        return isinstance(value, expected_type)


class ProjectConfig(ConfigPartition):
    class ConfigOptions(PoeOptions):
        """
        Options allowed directly under tool.poe in pyproject.toml
        """

        default_task_type: str = "cmd"
        default_array_task_type: str = "sequence"
        default_array_item_task_type: str = "ref"
        env: Mapping[str, str]
        envfile: Union[str, Sequence[str]]
        executor: Mapping[str, str]
        include: Union[str, Sequence[str], Mapping[str, str]]
        poetry_command: str
        poetry_hooks: Mapping[str, str]
        shell_interpreter: Union[str, Sequence[str]] = "posix"
        verbosity: int = 0
        tasks: Mapping[str, Any]


class IncludedConfig(ConfigPartition):
    class ConfigOptions(PoeOptions):
        env: Dict[str, str]
        envfile: Union[str, List[str]]
        tasks: Dict[str, Any]


class PoeConfig:
    cwd: Path
    _project_config: ProjectConfig
    _included_config: List[IncludedConfig]

    KNOWN_SHELL_INTERPRETERS = (
        "posix",
        "sh",
        "bash",
        "zsh",
        "fish",
        "pwsh",  # powershell >= 6
        "powershell",  # any version of powershell
        "python",
    )

    _config_name: str
    """
    The parent directory of the project config file
    """
    _project_dir: Optional[Path]
    """
    This can be overridden, for example to align with poetry
    """

    def __init__(
        self,
        cwd: Optional[Union[Path, str]] = None,
        table: Optional[Mapping[str, Any]] = None,
        config_name: str = "pyproject.toml",
    ):
        self.cwd = Path().resolve() if cwd is None else Path(cwd)
        self._project_config = ProjectConfig({"tool.poe": table or {}}, cwd=self.cwd)
        self._included_config = []
        self._config_name = config_name
        self._project_dir = self.cwd

    @property
    def executor(self) -> Mapping[str, Any]:
        return self._project_config.get("executor", {"type": "auto"})

    @property
    def task_names(self) -> Tuple[str]:
        return tuple(self.tasks.keys())

    @property
    def tasks(self) -> Mapping[str, Any]:  # TODO: deprecate raw access to tasks!!
        result = {}
        for config in reversed(self._included_config):
            result.update(config.get("tasks"))
        result.update(self._project_config.get("tasks"))
        return result

    @property
    def default_task_type(self) -> str:
        return self._project_config.options.default_task_type

    @property
    def default_array_task_type(self) -> str:
        return self._project_config.options.default_array_task_type

    @property
    def default_array_item_task_type(self) -> str:
        return self._project_config.options.default_array_item_task_type

    @property
    def global_env(self) -> Dict[str, Union[str, Dict[str, str]]]:
        return self._project_config.get("env")

    @property
    def global_envfile(self) -> Optional[str]:
        return self._project_config.get("envfile", None)

    @property
    def shell_interpreter(self) -> Tuple[str, ...]:
        raw_value = self._project_config.options.shell_interpreter
        if isinstance(raw_value, list):
            return tuple(raw_value)
        return (raw_value,)

    @property
    def verbosity(self) -> int:
        return self._project_config.options.verbosity

    @property
    def project(self) -> dict:
        return self._project_config.full_config

    @property
    def project_dir(self) -> str:
        return str(self._project_dir or self.cwd)

    def load(self, target_dir: Optional[str] = None):
        if self._project_config.get("tasks"):
            if not self._included_config:
                self._load_includes()
            return

        config_path = self.find_config_file(target_dir)
        self._project_dir = config_path.parent

        try:
            self._project_config = ProjectConfig(
                self._read_config_file(config_path),
                cwd=self._project_dir,
                path=config_path,
            )
        except KeyError:
            raise PoeException(
                f"No poe configuration found in file at {self._config_name}"
            )

        self._load_includes()

    def has_task(self, name: str):
        return name in self.tasks

    def validate(self):
        # TODO: validate included configs too!!                                    #####
        self._validate_config()

    def _validate_config(self):
        from .executor import PoeExecutor
        from .task import PoeTask

        supported_options = self._project_config.ConfigOptions.get_fields()
        raw_poe_config = self._project_config.poe_options

        # Validate keys
        unsupported_keys = set(raw_poe_config) - set(supported_options)
        if unsupported_keys:
            raise PoeException(f"Unsupported keys in poe config: {unsupported_keys!r}")

        # Validate types of option values
        for key in supported_options.keys():
            if key in raw_poe_config and not self._project_config.is_option_type_valid(
                key
            ):
                raise PoeException(
                    f"Unsupported value for option {key!r}, expected type to match "
                    f"{option_type!r}."
                )

        # Validate executor config
        error = PoeExecutor.validate_config(self.executor)
        if error:
            raise PoeException(error)

        # Validate default_task_type value
        if not PoeTask.is_task_type(self.default_task_type, content_type=str):
            raise PoeException(
                "Unsupported value for option `default_task_type` "
                f"{self.default_task_type!r}"
            )

        # Validate default_array_task_type value
        if not PoeTask.is_task_type(self.default_array_task_type, content_type=list):
            raise PoeException(
                "Unsupported value for option `default_array_task_type` "
                f"{self.default_array_task_type!r}"
            )

        # Validate default_array_item_task_type value
        if not PoeTask.is_task_type(self.default_array_item_task_type):
            raise PoeException(
                "Unsupported value for option `default_array_item_task_type` "
                f"{self.default_array_item_task_type!r}"
            )

        # Validate env value
        for key, value in self.global_env.items():
            if isinstance(value, dict):
                if tuple(value.keys()) != ("default",) or not isinstance(
                    value["default"], str
                ):
                    raise PoeException(
                        f"Invalid declaration at {key!r} in option `env`: {value!r}"
                    )
            elif not isinstance(value, str):
                raise PoeException(
                    f"Value of {key!r} in option `env` should be a string, but found "
                    f"{type(value)!r}"
                )

        # Validate tasks
        for task_name, task_def in self.tasks.items():
            error = PoeTask.validate_def(task_name, task_def, self)
            if error is None:
                continue
            raise PoeException(error)

        # Validate shell_interpreter type
        for interpreter in self.shell_interpreter:
            if interpreter not in self.KNOWN_SHELL_INTERPRETERS:
                raise PoeException(
                    f"Unsupported value {interpreter!r} for option `shell_interpreter`."
                )

        # Validate default verbosity.
        if self.verbosity < -1 or self.verbosity > 2:
            raise PoeException(
                f"Invalid value for option `verbosity`: {self.verbosity!r}. "
                "Should be between -1 and 2."
            )

    def find_config_file(self, target_dir: Optional[str] = None) -> Path:
        """
        Resolve a path to a self._config_name using one of two strategies:
          1. If target_dir is provided then only look there, (accept path to config file
             or to a directory).
          2. Otherwise look for the self._config_name in the current working directory,
             following by all parent directories in ascending order.

        Both strategies result in an Exception on failure.
        """
        if target_dir:
            target_path = Path(target_dir).resolve()
            if not (
                target_path.name.endswith(".toml") or target_path.name.endswith(".json")
            ):
                target_path = target_path.joinpath(self._config_name)
            if not target_path.exists():
                raise PoeException(
                    f"Poe could not find a {self._config_name} file at the given "
                    f"location: {target_dir}"
                )
            return target_path

        maybe_result = self.cwd.joinpath(self._config_name)
        while not maybe_result.exists():
            if len(maybe_result.parents) == 1:
                raise PoeException(
                    f"Poe could not find a {self._config_name} file in {self.cwd} or"
                    " its parents"
                )
            maybe_result = maybe_result.parents[1].joinpath(self._config_name).resolve()
        return maybe_result

    def _load_includes(self):
        include_option: Union[str, Sequence[str]] = self._project_config.get(
            "include", None
        )

        # Normalize includes configuration
        includes: List[Dict[str, str]] = []
        if isinstance(include_option, str):
            includes.append({"path": include_option})
        elif isinstance(include_option, dict):
            includes.append(include_option)
        elif isinstance(include_option, list):
            valid_keys = {"path", "cwd"}
            for include in include_option:
                if isinstance(include, str):
                    includes.append({"path": include})
                elif (
                    isinstance(include, dict)
                    and include.get("path")
                    and set(include.keys()) <= valid_keys
                ):
                    includes.append(include)
                else:
                    raise PoeException(
                        f"Invalid item for the include option {include!r}"
                    )

        # Attempt to load each of the included configs
        for include in includes:
            include_path = self._project_dir.joinpath(include["path"]).resolve()

            if not include_path.exists():
                # TODO: print warning in verbose mode, requires access to ui somehow
                continue

            try:
                self._included_config.append(
                    IncludedConfig(
                        self._read_config_file(include_path),
                        cwd=include.get("cwd", self.project_dir),
                        path=include_path,
                    )
                )
                # include_config = PoeConfig(
                #     cwd=include.get("cwd", self.project_dir),
                #     table=self._read_config_file(include_path)["tool"]["poe"],
                # )
                # include_config._project_dir = self._project_dir
            except (PoeException, KeyError) as error:
                raise PoeException(
                    f"Invalid content in included file from {include_path}", error
                ) from error

    # def _merge_config(self, include_config: "PoeConfig"):  # TODO: DELETE THIS
    #     from .task import PoeTask

    #     ## PROBLEMS ##
    #     # - should include.cwd dictate how we look for envfile??
    #     #   - YES: so we can use the .env from the target project area
    #     #   - NO: because we're working in the root project
    #     #   - OR: envfile should only apply to included tasks??  ...  ??
    #     #       - breaking change... but makes sense?
    #     #       - configurable within included file: global.env global.envfile   <---=
    #     #   - COMPS:
    #     #       - included envfile has can be overridden by envfile from root
    #     #            (explain rationale in docs)
    #     #       - yes only if "cwd=True" in included file (this sounds dumb)
    #     #   - ??
    #     #       - do we also need an option to isolate included tasks from root env??
    #     #           - naaa
    #     # - include.cwd how to keep track of task connection to included config file?
    #     #       - so task can prefer the env from the included config...

    #     """
    #     include.cwd should apply to
    #     - imported tasks
    #     - file level envfiles
    #     - task
    #         - level envfiles
    #         - capture_stdout

    #     task_def, task_inheritance = config.get_task(task_name)

    #     """

    #     # Env is special because it can be extended rather than just overwritten
    #     if include_config.global_env:
    #         self._poe["env"] = {**include_config.global_env, **self._poe.get("env", {})}

    #     if include_config.global_envfile and "envfile" not in self._poe:
    #         self._poe[
    #             "envfile"
    #         ] = (  ## FIXME: if envfile in root config then included envfile ignored??
    #             include_config.global_envfile
    #         )

    #     # Includes additional tasks with preserved ordering
    #     self._poe["tasks"] = own_tasks = self._poe.get("tasks", {})
    #     for task_name, task_def in include_config.tasks.items():
    #         if task_name in own_tasks:
    #             # don't override tasks from the base config
    #             continue

    #         task_def = PoeTask.normalize_task_def(task_def, include_config)
    #         if include_config.cwd:
    #             # Override the config of each task to use the include level cwd as a
    #             # base for the task level cwd
    #             if "cwd" in task_def:
    #                 # rebase the configured cwd onto the include level cwd
    #                 task_def["cwd"] = str(
    #                     Path(include_config.cwd)
    #                     .resolve()
    #                     .joinpath(task_def["cwd"])
    #                     .relative_to(self.project_dir)
    #                 )
    #             else:
    #                 task_def["cwd"] = str(include_config.cwd)

    #         own_tasks[task_name] = task_def

    @staticmethod
    def _read_config_file(path: Path) -> Mapping[str, Any]:
        try:
            with path.open("rb") as file:
                if path.suffix.endswith(".json"):
                    return json.load(file)
                else:
                    return tomli.load(file)

        except tomli.TOMLDecodeError as error:
            raise PoeException(f"Couldn't parse toml file at {path}", error) from error

        except json.decoder.JSONDecodeError as error:
            raise PoeException(
                f"Couldn't parse json file from {path}", error
            ) from error

        except Exception as error:
            raise PoeException(f"Couldn't open file at {path}") from error
