grails {
    mail {
        host = 'smtp.gmail.com'
        port = 465
        username = 'youracount@gmail.com'
        password = 'yourpassword'
        props = [
                'mail.smtp.auth'                  : 'true',
                'mail.smtp.socketFactory.port'    : '465',
                'mail.smtp.socketFactory.class'   : 'javax.net.ssl.SSLSocketFactory',
                'mail.smtp.socketFactory.fallback': 'false'
        ]
    }
}

// Added by the Spring Security Core plugin:
grails.plugin.springsecurity.userLookup.userDomainClassName = 'auth.User0'
grails.plugin.springsecurity.userLookup.authorityJoinClassName = 'auth.UserRole'
grails.plugin.springsecurity.authority.className = 'auth.Role0'
grails.plugin.springsecurity.controllerAnnotations.staticRules = [
	[pattern: '/',               access: ['permitAll']],
	[pattern: '/error',          access: ['permitAll']],
	[pattern: '/index',          access: ['permitAll']],
	[pattern: '/index.gsp',      access: ['permitAll']],
	[pattern: '/shutdown',       access: ['permitAll']],
	[pattern: '/assets/**',      access: ['permitAll']],
	[pattern: '/**/js/**',       access: ['permitAll']],
	[pattern: '/**/css/**',      access: ['permitAll']],
	[pattern: '/**/images/**',   access: ['permitAll']],
	[pattern: '/**/favicon.ico', access: ['permitAll']]
]

grails.plugin.springsecurity.filterChain.chainMap = [
	[pattern: '/assets/**',      filters: 'none'],
	[pattern: '/**/js/**',       filters: 'none'],
	[pattern: '/**/css/**',      filters: 'none'],
	[pattern: '/**/images/**',   filters: 'none'],
	[pattern: '/**/favicon.ico', filters: 'none'],
	[pattern: '/**',             filters: 'JOINED_FILTERS']
]

// RestApi
// Basic Authentication requires realm name. We recommend the app name.
// grails.plugin.springsecurity.useBasicAuth = true
// grails.plugin.springsecurity.basic.realmName = "Hubbub"
// grails.plugin.springsecurity.filterChain.chainMap = [
// 		'/api/**': 'JOINED_FILTERS',
// 		'/**': 'JOINED_FILTERS,-basicAuthenticationFilter, -basicExceptionTranslationFilter'
// ]
// grails.plugin.springsecurity.auth.loginFormUrl = "/login/form"
// grails.plugin.springsecurity.successHandler.defaultTargetUrl = "/timeline"
