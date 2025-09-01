from abc import ABC
import re
import time

from app.utils.model_utils import num_tokens_from_string

class Base(ABC):
    def __init__(self, key, model_name, base_url):
        pass

    def tts(self, audio, voice):
        pass

    def normalize_text(self, text):
        return re.sub(r'(\*\*|##\d+\$\$|#)', '', text)


class QwenTTS(Base):
    def __init__(self, key, model_name, base_url=""):
        import dashscope

        self.model_name = model_name
        dashscope.api_key = key

    def tts(self, text):
        from dashscope.api_entities.dashscope_response import SpeechSynthesisResponse
        from dashscope.audio.tts import ResultCallback, SpeechSynthesizer, SpeechSynthesisResult
        from collections import deque

        class Callback(ResultCallback):
            def __init__(self) -> None:
                self.dque = deque()

            def _run(self):
                while True:
                    if not self.dque:
                        time.sleep(0)
                        continue
                    val = self.dque.popleft()
                    if val:
                        yield val
                    else:
                        break

            def on_open(self):
                pass

            def on_complete(self):
                self.dque.append(None)

            def on_error(self, response: SpeechSynthesisResponse):
                raise RuntimeError(str(response))

            def on_close(self):
                pass

            def on_event(self, result: SpeechSynthesisResult):
                if result.get_audio_frame() is not None:
                    self.dque.append(result.get_audio_frame())

        text = self.normalize_text(text)
        callback = Callback()
        SpeechSynthesizer.call(model=self.model_name,
                               text=text,
                               callback=callback,
                               voice=self.voice,
                               format="mp3")
        try:
            for data in callback._run():
                yield data
            yield num_tokens_from_string(text)
        
        except Exception as e:
            raise RuntimeError(f"**ERROR**: {e}")
        
