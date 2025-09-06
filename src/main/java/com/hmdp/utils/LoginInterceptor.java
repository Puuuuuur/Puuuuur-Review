package com.hmdp.utils;

import com.hmdp.entity.User;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class LoginInterceptor implements HandlerInterceptor {


    /**
     * 登录检查
     * @param request
     * @param response
     * @param handler
     * @return
     * @throws Exception
     */
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 1.获取session
        HttpSession session = request.getSession();

        //获取session中的用户
        Object user = session.getAttribute("user");

        //判断用户是否存在
        if (user == null) {
            //不存在，拦截
            response.setStatus(401);
            return false;
        }

        //存在，保存用户信息到ThreadLocal
        UserHolder.saveUser((User) user);

        //放行
        return true;
    }

    /**
     * 请求处理之后进行调用，但是在视图被渲染之前（Controller方法调用之后）
     * @param request
     * @param response
     * @param handler
     * @param ex
     * @throws Exception
     */
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        // 移除用户信息，避免内存泄漏
        UserHolder.removeUser();
    }
}
