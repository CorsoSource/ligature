from ligature.simulators.mixins.support import MixinFunctionSupport
from ligature.easing import Easing


class EasingMixin(MixinFunctionSupport):
	
	# Required overrides for mixin functions

	def _configure_default_(self, variable):
		return {
			'start': self._variables[variable],
			'time_start': self._variables.get(self._escapement_variable, self._DEFAULT_START_VALUE),
		}
	
	def _configure_function_(self, **configuration):
		return Easing(**configuration)
