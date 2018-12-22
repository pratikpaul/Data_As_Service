package com.self.dataAsService

import javax.servlet.ServletContextListener
import javax.servlet.ServletContextEvent
import org.apache.shiro.web.util.WebUtils
import org.apache.shiro.SecurityUtils


class ShiroContextListener extends ServletContextListener {
  
  def contextInitialized(evt: ServletContextEvent) {
    println("ShiroContextListener contextInitialized ")
    val servletContext = evt.getServletContext
    val webEnv = WebUtils.getRequiredWebEnvironment( servletContext )
    SecurityUtils.setSecurityManager( webEnv.getWebSecurityManager )
  }

  def contextDestroyed(e: ServletContextEvent) { println("ShiroContextListener contextDestroyed ") }
}