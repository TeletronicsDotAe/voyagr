package org.apache.solr.servlet.security;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class RegExpAuthorizationFilter implements Filter {
  
  final static Logger log = LoggerFactory.getLogger(RegExpAuthorizationFilter.class);

  public static class RegExpPatternAndRoles {
    public int order;
    public String name;
    public Pattern regExpPattern;
    public String[] roles;
  }
  private List<RegExpPatternAndRoles> authorizationConstraints;
  
  @Override
  public void destroy() {}

  @Override
  public void init(FilterConfig config) throws ServletException {
    Map<String, String> initParamsMap = new HashMap<String, String>();
    Enumeration<String> initParamNames = config.getInitParameterNames();
    while (initParamNames.hasMoreElements()) {
      String name = initParamNames.nextElement();
      String value = config.getInitParameter(name);
      initParamsMap.put(name, value);
    }
    authorizationConstraints = getAuthorizationConstraints(initParamsMap);
  }
  
  public static List<RegExpPatternAndRoles> getAuthorizationConstraints(Map<String, String> keyValueMap) {
    List<RegExpPatternAndRoles> authorizationConstraints = new ArrayList<RegExpPatternAndRoles>();
    for (Map.Entry<String, String> keyValue : keyValueMap.entrySet()) {
      String name = keyValue.getKey();
      String value = keyValue.getValue();
      int orderRolesSplit = value.indexOf("|");
      int rolesPatternSplit = value.indexOf("|", orderRolesSplit+1);
      // Do not use StringUtils.split here because pattern might contain | (roles are not allowed to for this filter to work)
      String[] orderRolesAndPattern = new String[]{value.substring(0, orderRolesSplit), value.substring(orderRolesSplit+1, rolesPatternSplit), value.substring(rolesPatternSplit+1)};
      RegExpPatternAndRoles regExpPatternAndRoles = new RegExpPatternAndRoles();
      regExpPatternAndRoles.order = Integer.parseInt(orderRolesAndPattern[0].trim());
      regExpPatternAndRoles.name = name;
      regExpPatternAndRoles.roles = StringUtils.split(orderRolesAndPattern[1], ","); 
      String regExp = orderRolesAndPattern[2].trim();
      regExpPatternAndRoles.regExpPattern = Pattern.compile(regExp);
      authorizationConstraints.add(regExpPatternAndRoles);
      log.debug("Added authorization constraint - order: " + regExpPatternAndRoles.order + ", name: " + name + ", reg-exp: " + regExp + ", authorized-roles: " + printRoles(regExpPatternAndRoles.roles));
    }
    Collections.sort(authorizationConstraints, new Comparator<RegExpPatternAndRoles>() {
      @Override
      public int compare(RegExpPatternAndRoles a, RegExpPatternAndRoles b) {
        return a.order - b.order;
      }
    });
    return authorizationConstraints;
  }

  @Override
  public void doFilter(ServletRequest req, ServletResponse resp,
      FilterChain chain) throws IOException, ServletException {
    if (req instanceof HttpServletRequest) {
      HttpServletRequest httpReq = (HttpServletRequest)req;
      String servletPath = httpReq.getServletPath();
      // httpReq.getPathInfo() seems to work on a "test" jetty set up by JettySolrRunner - httpReq.getServletPath() doesnt
      if (StringUtils.isEmpty(servletPath)) servletPath = httpReq.getPathInfo();
      
      List<String> firstMatchingConstraintsRoles = getMatchingRoles(authorizationConstraints, servletPath);
      boolean ok = false;
      for (int i = 0; i < firstMatchingConstraintsRoles.size(); i++) {
        if (httpReq.isUserInRole(firstMatchingConstraintsRoles.get(i))) {
          ok = true;
          break;
        }
      }
      
      if (!ok) {
        Principal principal = httpReq.getUserPrincipal();
        log.debug(((principal != null)?principal.getName():"Unauthenticated user") + " tried to access unauthorized resource " + servletPath);
        ((HttpServletResponse)resp).sendError(403, "Unauthorized");
        return;
      }
    }
    
    chain.doFilter(req, resp);
  }
  
  public static List<String> getMatchingRoles(List<RegExpPatternAndRoles> authorizationConstraints, String path) {
    List<String> result = new ArrayList<String>();
    outerLoop: for (RegExpPatternAndRoles regExpPatternAndRoles : authorizationConstraints) {
      Matcher matcher = regExpPatternAndRoles.regExpPattern.matcher(path);
      if (matcher.find()) {
        for (int i = 0; i < regExpPatternAndRoles.roles.length; i++) {
          result.add(regExpPatternAndRoles.roles[i]);
        }
        // It is by intent that we do not return more than the roles from the first matching constraint
        // because that is the way security-constraints in deployment descriptors (web.xml) works, and guess its nice to have similar behavior
        break;
      }
    }
    StringBuilder sb = new StringBuilder();
    for (String role : result) {
      sb.append(" ").append(role);
    }
    log.debug("Roles allowed to access " + path + ":" + sb.toString());
    return result;
  }
  
  private static String printRoles(String[] roles) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < roles.length; i++) {
      if (i > 0) sb.append(" ");
      sb.append(roles[i]);
    }
    return sb.toString();
  }

}
