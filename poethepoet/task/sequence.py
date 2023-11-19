from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
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


class SequenceTask(PoeTask):
    """
    A task consisting of a sequence of other tasks
    """

    content: List[Union[str, Dict[str, Any]]]

    __key__ = "sequence"
    __content_type__: Type = list

    class TaskOptions(PoeTask.TaskOptions):
        ignore_fail: Union[bool, str]
        default_item_type: str

    class TaskSpec(PoeTask.TaskSpec[TaskOptions]):
        subtasks: Sequence[PoeTask.TaskSpec]

        def __init__(
            self,
            name: str,
            task_def: TaskDef,
            task_type: Type["PoeTask"],
            config: "PoeConfig",
        ):
            super().__init__(name, task_def, task_type, config)

            self.subtasks = []
            for index, sub_task_def in enumerate(task_def[task_type.__key__]):
                # TODO: avoid repeating this logic to get subtask_type here?
                task_type_key = task_type.resolve_task_type(
                    sub_task_def, config, array_item=True
                )
                subtask_type = task_type.resolve_task_cls(task_type_key)
                if not isinstance(sub_task_def, dict):
                    sub_task_def = {task_type_key: sub_task_def}

                self.subtasks.append(
                    subtask_type.get_task_spec(
                        SequenceTask._subtask_name(name, index), sub_task_def, config
                    )
                )

    def __init__(
        self,
        spec: TaskSpec,
        invocation: Tuple[str, ...],
        ui: "PoeUi",
        config: "PoeConfig",
        capture_stdout: bool = False,
        inheritance: Optional[TaskInheritance] = None,
    ):
        assert capture_stdout in (False, None)  # TODO: tidy this?
        super().__init__(spec, invocation, ui, config, False, inheritance)

        self.subtasks = [
            self.from_spec(
                task_spec=task_spec,
                invocation=(task_spec.name,),
                config=config,
                ui=ui,
                array_item=self.spec.options.get("default_item_type", True),
                inheritance=TaskInheritance.from_task(self),
            )
            for task_spec in spec.subtasks
        ]

    # @classmethod
    # def get_task_spec(
    #     cls, name: str, task_def: Dict[str, Any], config: "PoeConfig"
    # ) -> TaskSpec:
    #     subtasks = []
    #     for index, sub_task_def in enumerate(task_def[cls.__key__]):
    #         # TODO: avoid repeating this logic here
    #         task_type_key = cls.resolve_task_type(sub_task_def, config, array_item=True)
    #         task_type = cls.resolve_task_cls(task_type_key)
    #         if not isinstance(sub_task_def, dict):
    #             sub_task_def = {task_type_key: sub_task_def}

    #         subtasks.append(
    #             task_type.get_task_spec(
    #                 self._subtask_name(name, index), sub_task_def, config
    #             )
    #         )

    #     return TaskSpec(
    #         name=name,
    #         content=None,
    #         options=cls.TaskOptions(task_def),
    #         task_type=cls,
    #         subtasks=subtasks,
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
            raise PoeException(f"Sequence task {self.name!r} does not accept arguments")

        if len(self.subtasks) > 1:
            # Indicate on the global context that there are multiple stages
            context.multistage = True

        ignore_fail = self.spec.options.get("ignore_fail")
        non_zero_subtasks: List[str] = list()
        for subtask in self.subtasks:
            task_result = subtask.run(
                context=context, extra_args=tuple(), parent_env=env
            )
            if task_result and not ignore_fail:
                raise ExecutionError(
                    f"Sequence aborted after failed subtask {subtask.name!r}"
                )
            if task_result:
                non_zero_subtasks.append(subtask.name)

        if non_zero_subtasks and ignore_fail == "return_non_zero":
            raise ExecutionError(
                f"Subtasks {', '.join(non_zero_subtasks)} returned non-zero exit status"
            )
        return 0

    @classmethod
    def _subtask_name(cls, task_name: str, index: int):
        return f"{task_name}[{index}]"

    @classmethod
    def _validate_task_def(
        cls, task_name: str, task_def: Dict[str, Any], config: "PoeConfig"
    ) -> Optional[str]:
        default_item_type = task_def.get("default_item_type")
        if default_item_type is not None and not cls.is_task_type(
            default_item_type, content_type=str
        ):
            return (
                "Unsupported value for option `default_item_type` for task "
                f"{task_name!r}. Expected one of {cls.get_task_types(content_type=str)}"
            )

        ignore_fail = task_def.get("ignore_fail")
        if ignore_fail is not None and ignore_fail not in (
            True,
            False,
            "return_zero",
            "return_non_zero",
        ):
            return (
                f"Unsupported value for option `ignore_fail` for task {task_name!r}."
                ' Expected one of (true, false, "return_zero", "return_non_zero")'
            )

        for index, task_item in enumerate(task_def["sequence"]):
            if isinstance(task_item, dict):
                if len(task_item.get("args", tuple())):
                    return (
                        "Unsupported option `args` for task declared inside sequence "
                        f"task {task_name!r}."
                    )

                subtask_issue = cls.validate_def(
                    cls._subtask_name(task_name, index),
                    task_item,
                    config,
                    anonymous=True,
                )
                if subtask_issue:
                    return subtask_issue

            else:
                subtask_issue = cls.validate_def(
                    cls._subtask_name(task_name, index),
                    cls.normalize_task_def(
                        task_item,
                        config,
                        array_item=default_item_type or True,
                    ),
                    config,
                    anonymous=True,
                )
                if subtask_issue:
                    return subtask_issue

        return None
