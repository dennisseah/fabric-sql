from typing import Any, Literal

from openai.types.chat import (
    ChatCompletion,
)

from fabric_sql.protocols.i_openai_content_evaluator import (
    ContentSafeException,
    IOpenAIContentEvaluator,
)


class OpenAIContentEvaluator(IOpenAIContentEvaluator):
    def evaluate_severity(
        self, dict_filters: dict[str, str], theshold: Literal["low", "medium", "high"]
    ) -> None:
        if "severity" in dict_filters:
            lc_val = dict_filters["severity"].lower()

            if (
                (lc_val == "high")
                or (lc_val == "medium" and theshold in ["medium", "low"])
                or (lc_val == "low" and theshold == "low")
            ):
                raise ContentSafeException(
                    f"Content safety check failed. Severity: {lc_val}."
                )

    def validate(
        self, data: dict[str, Any], threshold: Literal["low", "medium", "high"]
    ) -> None:
        for k, v in data.items():
            if (
                ("filtered" in v and v["filtered"] is True)
                or self.evaluate_severity(v, threshold)
                or ("detected" in v and v["detected"] is True)
            ):
                raise ContentSafeException(
                    f"Content safety check failed. Category: {k}."
                )

    def content_safety_check(
        self,
        response: ChatCompletion,
        threshold: Literal["low", "medium", "high"] = "high",
    ) -> None:
        if response.prompt_filter_results:  # type: ignore
            item: dict[str, Any] = response.prompt_filter_results[0]  # type: ignore

            if "content_filter_results" in item and item["content_filter_results"]:
                self.validate(item["content_filter_results"], threshold)  # type: ignore

            if (
                response.choices
                and "content_filter_results" in response.choices[0].model_extra  # type: ignore
            ):
                self.validate(
                    response.choices[0].model_extra["content_filter_results"],  # type: ignore
                    threshold,
                )  # type: ignore
