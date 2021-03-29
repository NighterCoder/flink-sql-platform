package com.flink.platform.web.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.platform.web.common.entity.login.LoginUser;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.DisabledException;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.core.userdetails.jdbc.JdbcDaoImpl;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.crypto.password.StandardPasswordEncoder;
import org.springframework.security.web.access.AccessDeniedHandler;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;
import org.springframework.util.AntPathMatcher;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.util.*;

//不需要@EnableWebSecurity注解
@Configuration
public class WebSecurityConfig extends WebSecurityConfigurerAdapter {

    @Autowired
    private ObjectMapper objectMapper;

    @Resource
    private JdbcTemplate jdbcTemplate;

    // 定义需要校验权限的路径
    // 其他的路径只要认证通过即可
    // private final String[] authPath = new String[]{"/auth/**", "/admin/**", "/api/**"};
    private final String[] authPath = new String[]{"/auth/**", "/admin/**"};

    private AntPathMatcher antPathMatcher = new AntPathMatcher();


    /**
     * 密码编辑器
     */
    @Bean
    public PasswordEncoder passwordEncoder() {
        return NoOpPasswordEncoder.getInstance();
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        //开启跨域访问
        http.cors().disable();
        //开启模拟请求，比如API POST测试工具的测试，不开启时，API POST为报403错误
        http.csrf().disable();
        http.headers().frameOptions().sameOrigin();


        // 访问控制
        // 登录验证之后,访问相关url验证当前用户是否有权限
        http.authorizeRequests().antMatchers(authPath).access("@webSecurityConfig.hasPermission(request,authentication)");
        http.authorizeRequests().antMatchers("/**").authenticated();

        //登录
        http.formLogin().permitAll()
                .successHandler(authenticationSuccessHandler())
                .failureHandler(authenticationFailureHandler());


        //授权
        http.exceptionHandling().accessDeniedHandler(accessDeniedHandler());

        //退出
        http.logout().permitAll().invalidateHttpSession(true);
    }


    @Override
    public void configure(WebSecurity web) {
        //对于在header里面增加token等类似情况，放行所有OPTIONS请求。
        web.ignoring().antMatchers(
                HttpMethod.OPTIONS,
                "/**"
        );
        web.ignoring().antMatchers(
                "/*.ico",
                "/libs/**",
                "/css/**",
                "/js/**",
                "/img/**",
                 "/api/**"   // 放开/api路径访问权限

        );
    }


    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        // 每次登陆都会加载用户信息服务查找
        LoginUserDetailsService userDetailsService = new LoginUserDetailsService();
        userDetailsService.setJdbcTemplate(jdbcTemplate);
        // 根据用户名查找当前用户的身份信息
        userDetailsService.setUsersByUsernameQuery("select username,password,enabled,id,root from auth_user where username = ?");
        // 根据用户名查找当前用户拥有权限汇总
        userDetailsService.setAuthoritiesByUsernameQuery("select username,role from auth_user_role where username = ?");
        auth.userDetailsService(userDetailsService).passwordEncoder(new StandardPasswordEncoder());
    }


    /**
     * 认证通过之后,查询当前权限
     */
    private AuthenticationSuccessHandler authenticationSuccessHandler() {
        final String defRoleResourceByRoleCode =
                "select rr.resource, re.url from auth_role_resource rr, auth_resource re where rr.resource = re.code and rr.role in (%s)";

        return (httpServletRequest, httpServletResponse, authentication) -> {
            LoginUser principal = (LoginUser) authentication.getPrincipal();
            // 非超级管理员则加载资源
            if (!principal.isRoot()) {
                // 加载用户资源
                StringBuilder roles = new StringBuilder();
                principal.getAuthorities().forEach(grantedAuthority -> roles.append("\'").append(grantedAuthority.getAuthority()).append("\'").append(","));
                String sql = String.format(defRoleResourceByRoleCode, roles.substring(0, roles.length() - 1));
                List<Map<String, Object>> roleResourcesList = jdbcTemplate.queryForList(sql);
                Map<String, String> roleResourcesMap = new HashMap<>();
                String contextPath = httpServletRequest.getContextPath();
                roleResourcesList.forEach(item -> {
                    String resource = item.get("resource") != null ? (String) item.get("resource") : "";
                    String url = "";
                    if (item.get("url") != null) {
                        url = StringUtils.join(
                                Arrays.stream(((String) item.get("url")).split(",")).map(u -> contextPath + u).toArray(),
                                ","
                        );
                    }
                    roleResourcesMap.put(resource, url);
                });
                principal.setResources(roleResourcesMap);

            }
            httpServletRequest.getSession().setAttribute("user", principal);
            httpServletResponse.setContentType("application/json;charset=UTF-8");
            Map<String, Object> map = new HashMap<>();
            map.put("code", 200);
            map.put("message", "登录成功");
            map.put("data", authentication);
            PrintWriter out = httpServletResponse.getWriter();
            out.write(objectMapper.writeValueAsString(map));
            out.flush();
            out.close();
        };
    }


    private AuthenticationFailureHandler authenticationFailureHandler() {
        return (request, response, ex) -> {
            response.setContentType("application/json;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            PrintWriter out = response.getWriter();
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("code", 401);
            if (ex instanceof UsernameNotFoundException || ex instanceof BadCredentialsException) {
                map.put("message", "用户名或密码错误");
            } else if (ex instanceof DisabledException) {
                map.put("message", "账户被禁用");
            } else {
                map.put("message", "登录失败!");
            }
            out.write(objectMapper.writeValueAsString(map));
            out.flush();
            out.close();
        };
    }

    private AccessDeniedHandler accessDeniedHandler() {
        return (request, response, ex) -> {
            response.setContentType("application/json;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            PrintWriter out = response.getWriter();
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("code", 403);
            map.put("message", "权限不足");
            out.write(objectMapper.writeValueAsString(map));
            out.flush();
            out.close();
        };
    }

    /**
     * 验证当前用户是否对访问url具备权限
     */
    public boolean hasPermission(HttpServletRequest request, Authentication authentication) {
        Object principal = authentication.getPrincipal();
        if (principal instanceof LoginUser) {
            if (((LoginUser) principal).isRoot()) {
                return true;
            }
            // 当前用户
            for (String url : ((LoginUser) principal).getResources().values()) {
                if (url.contains(",")) {
                    for (String partUrl : url.split(",")) {
                        if (antPathMatcher.match(partUrl, request.getRequestURI())) {
                            return true;
                        }
                    }
                } else {
                    if (antPathMatcher.match(url, request.getRequestURI())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }


    /**
     * 用户信息服务
     */
    class LoginUserDetailsService extends JdbcDaoImpl {

        /**
         * 返回用户信息,这里没有包含权限信息
         *
         * 返回的UserDetails放到Spring全局缓存SecurityContextHolder中
         * 之后在代码中可以使用 CustomUserDetails myUserDetails= (CustomUserDetails) SecurityContextHolder.getContext().getAuthentication() .getPrincipal();
         * 获取用户信息
         *
         * @param username 用户名
         */
        @Override
        protected List<UserDetails> loadUsersByUsername(String username) {
            return this.getJdbcTemplate().query(super.getUsersByUsernameQuery(), new String[]{username}, (rs, rowNum) -> {
                String name = rs.getString(1);
                String pwd = rs.getString(2);
                boolean enabled = rs.getBoolean(3);
                String id = rs.getString(4);
                boolean root = rs.getBoolean(5);
                return new LoginUser(id, root, name, pwd, enabled, true, true, true, AuthorityUtils.NO_AUTHORITIES);
            });
        }

        /**
         * 返回当前用户的权限信息
         *
         * @param username 用户名
         */
        @Override
        protected List<GrantedAuthority> loadUserAuthorities(String username) {
            List<GrantedAuthority> authorities = super.loadUserAuthorities(username);
            if (authorities.isEmpty()) {
                return Collections.singletonList(new SimpleGrantedAuthority("no_auth"));
            }
            return authorities;
        }

        @Override
        protected UserDetails createUserDetails(String username, UserDetails userFromUserQuery, List<GrantedAuthority> combinedAuthorities) {
            String returnUsername = userFromUserQuery.getUsername();
            if (!isUsernameBasedPrimaryKey()) {
                returnUsername = username;
            }
            String id = ((LoginUser) userFromUserQuery).getId();
            boolean root = ((LoginUser) userFromUserQuery).isRoot();
            return new LoginUser(id, root, returnUsername, userFromUserQuery.getPassword(), userFromUserQuery.isEnabled(), true, true, true, combinedAuthorities);
        }
    }


}
