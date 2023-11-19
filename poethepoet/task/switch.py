from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

from ..exceptions import ExecutionError, PoeException
from .base import PoeTask, TaskDef, TaskInheritance

if TYPE_CHECKING:
    from ..config import PoeConfig
    from ..context import RunContext
    from ..env.manager import EnvVarsManager
    from ..ui import PoeUi


DEFAULT_CASE = "__default__"


# Do we need to subclass TaskSpec for each task type???
# So that custom task spec creation logic can live on the TaskSpec subclass!!!


class SwitchTask(PoeTask):
    """
    A task that runs one of several `case` subtasks depending on the output of a
    `switch` subtask.
    """

    __key__ = "switch"
    __content_type__: Type = list

    class TaskOptions(PoeTask.TaskOptions):
        control: Union[str, dict]
        default: str

    class TaskSpec(PoeTask.TaskSpec[TaskOptions]):
        control_spec: PoeTask.TaskSpec
        case_specs: Mapping[Tuple[Any, ...], PoeTask.TaskSpec]

        def __init__(
            self,
            name: str,
            task_def: TaskDef,
            task_type: Type["PoeTask"],
            config: "PoeConfig",
        ):
            super().__init__(name, task_def, task_type, config)

            switch_args = task_def.get("args")

            control_task_def = task_def["control"]
            control_task_type_key = task_type.resolve_task_type(
                control_task_def, config
            )
            control_task_type = task_type.resolve_task_cls(control_task_type_key)
            if not isinstance(control_task_def, dict):
                control_task_def = {control_task_type_key: control_task_def}
            if switch_args:
                control_task_def = {**control_task_def, "args": switch_args}
            self.control_spec = control_task_type.get_task_spec(
                f"{name}[control]", control_task_def, config
            )

            case_specs = {}
            for index, case_task_def in enumerate(task_def["switch"]):
                task_type_key = task_type.resolve_task_type(case_task_def, config)
                task_type = task_type.resolve_task_cls(task_type_key)

                if switch_args:
                    case_task_def = {**case_task_def, "args": switch_args}

                case_specs[
                    SwitchTask.get_case_keys(case_task_def)
                ] = task_type.get_task_spec(f"{name}[{index}]", case_task_def, config)

            self.case_specs = case_specs

    spec: TaskSpec
    control_task: PoeTask
    switch_tasks: Dict[str, PoeTask]

    def __init__(
        self,
        spec: TaskSpec,
        invocation: Tuple[str, ...],
        ui: "PoeUi",
        config: "PoeConfig",
        capture_stdout: bool = False,
        inheritance: Optional[TaskInheritance] = None,
    ):
        super().__init__(spec, invocation, ui, config, False, inheritance)

        control_invocation: Tuple[str, ...] = (spec.name,)
        if self.spec.options.get("args"):
            control_invocation = (*control_invocation, *invocation[1:])

        self.control_task = self.from_spec(
            spec=self.spec.control,
            invocation=control_invocation,
            config=config,
            ui=ui,
            capture_stdout=True,
            inheritance=TaskInheritance.from_task(self),
        )

        self.switch_tasks = {}
        for case_keys, case_spec in spec.case_specs.items():
            # task_def = {key: value for key, value in item.items() if key != "case"}

            task_invocation: Tuple[str, ...] = (name,)
            if self.spec.options.get("args"):
                task_invocation = (*task_invocation, *invocation[1:])

            case_task = self.from_spec(
                spec=case_spec,
                invocation=task_invocation,
                config=config,
                ui=ui,
                capture_stdout=self.options.get("capture_stdout", capture_stdout),
                inheritance=TaskInheritance.from_task(self),
            )
            for case_key in case_keys:
                self.switch_tasks[case_key] = case_task

    # @classmethod
    # def get_task_spec(
    #     cls, name: str, task_def: Dict[str, Any], config: "PoeConfig"
    # ) -> TaskSpec:
    #     switch_args = task_def.get("args")

    #     control_task_def = task_def["control"]
    #     control_task_type_key = cls.resolve_task_type(control_task_def, config)
    #     control_task_type = cls.resolve_task_cls(control_task_type_key)
    #     if not isinstance(control_task_def, dict):
    #         control_task_def = {control_task_type_key: control_task_def}
    #     if switch_args:
    #         control_task_def = {**control_task_def, "args": switch_args}
    #     control = control_task_type.get_task_spec(
    #         f"{name}[control]", control_task_def, config
    #     )

    #     task_content = {}
    #     for index, case_task_def in enumerate(task_def["switch"]):
    #         task_type_key = cls.resolve_task_type(case_task_def, config)
    #         task_type = cls.resolve_task_cls(task_type_key)

    #         if switch_args:
    #             case_task_def = {**case_task_def, "args": switch_args}

    #         task_content[
    #             case_task_def.get("case", DEFAULT_CASE)
    #         ] = task_type.get_task_spec(f"{name}[{index}]", case_task_def, config)

    #     return TaskSpec(
    #         name=name,
    #         content=task_content,
    #         options=cls.TaskOptions(dict(task_def, control=control)),
    #         task_type=cls,
    #     )

    def _handle_run(
        self,
        context: "RunContext",
        extra_args: Sequence[str],
        env: "EnvVarsManager",
    ) -> int:
        named_arg_values = self.get_named_arg_values(env)
        env.update(named_arg_values)

        if not named_arg_values and any(arg.strip() for arg in extra_args):
            raise PoeException(f"Switch task {self.name!r} does not accept arguments")

        # Indicate on the global context that there are multiple stages to this task
        context.multistage = True

        task_result = self.control_task.run(
            context=context,
            extra_args=extra_args if self.spec.options.get("args") else tuple(),
            parent_env=env,
        )
        if task_result:
            raise ExecutionError(
                f"Switch task {self.name!r} aborted after failed control task"
            )

        if context.dry:
            self._print_action(
                "unresolved case for switch task", dry=True, unresolved=True
            )
            return 0

        control_task_output = context.get_task_output(self.control_task.invocation)
        case_task = self.switch_tasks.get(
            control_task_output, self.switch_tasks.get(DEFAULT_CASE)
        )

        if case_task is None:
            if self.spec.options.get("default", "fail") == "pass":
                return 0
            raise ExecutionError(
                f"Control value {control_task_output!r} did not match any cases in "
                f"switch task {self.name!r}."
            )

        return case_task.run(context=context, extra_args=extra_args, parent_env=env)

    @classmethod
    def get_case_keys(cls, task_def: Dict[str, Any]) -> Tuple[Any, ...]:
        case_value = task_def.get("case", DEFAULT_CASE)
        if isinstance(case_value, list):
            return case_value
        return [case_value]

    @classmethod
    def _validate_task_def(
        cls, task_name: str, task_def: Dict[str, Any], config: "PoeConfig"
    ) -> Optional[str]:
        from collections import defaultdict

        control_task_def = task_def.get("control")
        if not control_task_def:
            return f"Switch task {task_name!r} has no control task."

        allowed_control_task_types = ("expr", "cmd", "script")
        if isinstance(control_task_def, dict) and not any(
            key in control_task_def for key in allowed_control_task_types
        ):
            return (
                f"Control task for {task_name!r} must have a type that is one of "
                f"{allowed_control_task_types!r}"
            )

        control_task_issue = PoeTask.validate_def(
            f"{task_name}[control]", control_task_def, config, anonymous=True
        )
        if control_task_issue:
            return control_task_issue

        cases: MutableMapping[Any, int] = defaultdict(int)
        for switch_task in task_def["switch"]:
            for case_key in cls.get_case_keys(switch_task):
                cases[case_key] += 1

            case_key = switch_task.get("case", DEFAULT_CASE)
            for invalid_option in ("args", "deps"):
                if invalid_option in switch_task:
                    if case_key is DEFAULT_CASE:
                        return (
                            f"Default case of switch task {task_name!r} includes "
                            f"invalid option {invalid_option!r}"
                        )
                    return (
                        f"Case {case_key!r} switch task {task_name!r} include invalid "
                        f"option {invalid_option!r}"
                    )

            switch_task_issue = PoeTask.validate_def(
                f"{task_name}[{case_key}]",
                switch_task,
                config,
                anonymous=True,
                extra_options=("case",),
            )
            if switch_task_issue:
                return switch_task_issue

        for case, count in cases.items():
            if count > 1:
                if case is DEFAULT_CASE:
                    return (
                        f"Switch task {task_name!r} includes more than one default case"
                    )
                return (
                    f"Switch task {task_name!r} includes more than one case for "
                    f"{case!r}"
                )

        if "default" in task_def:
            if task_def["default"] not in ("pass", "fail"):
                return (
                    f"The 'default' option for switch task {task_name!r} should be one "
                    "of ('pass', 'fail')"
                )
            if DEFAULT_CASE in cases:
                return (
                    f"Switch task {task_name!r} should not have both a default case "
                    f"and the 'default' option."
                )

        return None
