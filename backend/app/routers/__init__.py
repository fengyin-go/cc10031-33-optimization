"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import execution_plan, resource

ROUTERS = [execution_plan, resource]
