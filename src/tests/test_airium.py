from typing import Any, cast

from utilities.airium import yield_airium
from utilities.text import strip_and_dedent


class TestYieldAirium:
    def test_main(self) -> None:
        with yield_airium() as airium, cast(Any, airium).div():
            airium("hello")
        result = str(airium)
        expected = """
            <!DOCTYPE html>
            <html>
              <body>
                <div>
                  hello
                </div>
              </body>
            </html>
        """
        assert result == strip_and_dedent(expected)
