from dataclasses import dataclass, field

@dataclass
class CodeTree:
    seen_types:dict = field(default_factory=dict)
    seen_vars :dict= field(default_factory=dict)
    seen_procs:dict = field(default_factory=dict)