from __future__ import annotations

import re
from typing import Any, Dict, List, Union

# 匹配 [[ KEY ]] 或 [[KEY]]，忽略前后空格
# Group 1 是 KEY
SECRET_PATTERN = re.compile(r"\[\[\s*(\S+)\s*\]\]")


def resolve_secrets_in_params(params: Dict[str, Any], secret_map: Dict[str, str]) -> Dict[str, Any]:
    """
    递归遍历 params，将字符串值中的 [[ KEY ]] 替换为 secret_map[KEY]。
    
    Args:
        params: 节点的参数字典 (可能包含嵌套 dict/list)
        secret_map: 从 Context 中获取的 {KEY: VALUE} 字典
        
    Returns:
        替换后的新字典
    """
    if not params:
        return {}
    if not secret_map:
        return params

    resolved: Dict[str, Any] = {}
    
    for k, v in params.items():
        resolved[k] = _resolve_value(v, secret_map)
            
    return resolved


def _resolve_value(value: Any, secret_map: Dict[str, str]) -> Any:
    """内部递归辅助函数"""
    if isinstance(value, str):
        # 优化：如果没有 [[，直接返回，避免正则开销
        if "[[" not in value:
            return value
            
        def _replacer(match: re.Match) -> str:
            key = match.group(1)
            # 如果 Secret 存在，则替换
            # 如果不存在，保留原样 (方便调试，或者避免误杀)
            return secret_map.get(key, match.group(0))
        
        # 处理全量替换的特殊情况：如果字符串就是 "[[KEY]]"，且对应的值不是字符串（比如是数字或JSON对象）
        # 这种情况下，正则替换会强转为字符串。
        # 如果需要支持类型转换，需要精确匹配。
        # 目前主要支持字符串替换。
        full_match = SECRET_PATTERN.fullmatch(value.strip())
        if full_match:
            key = full_match.group(1)
            if key in secret_map:
                return secret_map[key]

        return SECRET_PATTERN.sub(_replacer, value)

    elif isinstance(value, dict):
        return {k: _resolve_value(v, secret_map) for k, v in value.items()}
    
    elif isinstance(value, list):
        return [_resolve_value(item, secret_map) for item in value]
        
    else:
        return value


# 废弃接口保留，防止硬编码导入报错
def resolve_secret_value(secret_id: str) -> str:
    raise NotImplementedError("Deprecated: Secrets are now resolved via context injection from MQ.")