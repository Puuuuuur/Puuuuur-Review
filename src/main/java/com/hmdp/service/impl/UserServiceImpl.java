package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.core.toolkit.BeanUtils;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 发送验证码
     *
     * @param phone  手机号
     * @param session session
     * @return {@link Result}
     */
    public Result sendCode(String phone, HttpSession session) {
        //校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            //如果不符合，返回错误信息
            return Result.fail("手机号格式错误!");
        }

        //符合，生成验证码
        String code = RandomUtil.randomNumbers(6);

        //保存验证码到redis  // set key value ex 120
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY + phone, code, RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES);

        //发送验证码
        log.debug("发送短信验证码成功，验证码:{}", code);

        //返回ok
        return Result.ok("发送成功");
    }

    /**
     * 登录功能
     *
     * @param loginForm 登录参数，包含手机号、验证码、密码
     * @param session   session
     * @return {@link Result}
     */
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //校验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            //如果不符合，返回错误信息
            return Result.fail("手机号格式错误!");
        }

        //从redis获取验证码并校验
        String cacheCode = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (cacheCode == null || !cacheCode.toString().equals(code)) {
            //不一致，报错
            return Result.fail("验证码错误!");
        }

        //一致，根据手机号查询用户 select * from tb_user where phone = ?
        User user = query().eq("phone", phone).one();

        //判断用户是否存在
        if (user == null) {
            //不存在，创建新用户并保存
            user = createUserWithPhone(phone);
        }
        //存在，随机生成token，作为登录令牌
        String token = UUID.randomUUID().toString(true);

        //将User对象转为Hash存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = new HashMap<>();
        userMap.put("id", userDTO.getId().toString());
        userMap.put("nickName", userDTO.getNickName());
        userMap.put("icon", userDTO.getIcon());

        //保存用户信息到redis
        String tokenKey = RedisConstants.LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        stringRedisTemplate.expire(tokenKey, RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);

        //返回token
        return Result.ok(token);
    }

    /**
     * 用户签到
     *
     * @return {@link Result}
     */
    public Result sign() {
        ///获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //获取日期
        LocalDateTime now = LocalDateTime.now();
        //拼接key
        String keySuffix = RedisConstants.USER_SIGN_KEY + userId + now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        //获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //写入redis  SETBIT key offset 1
        stringRedisTemplate.opsForValue().setBit(keySuffix, dayOfMonth - 1, true);
        return Result.ok();
    }

    private User createUserWithPhone(String phone) {
        //创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        //保存用户
        save(user);
        return user;
    }
}
