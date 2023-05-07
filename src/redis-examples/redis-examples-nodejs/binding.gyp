{
    'targets' :[
        {
            'target_name': 'krb5auth',
            'type': 'loadable_module',
            'include_dirs': [  "<!(node -p \"require('node-addon-api').include_dir\")" ],
            'sources': [
                'src/kerberos.cpp'
            ],
            'link_settings': {
                'libraries': [
                    '-lkrb5',
                    '-lsasl2'
                ]
            },
            'xcode_settings': {
                'GCC_ENABLE_CPP_EXCEPTIONS': 'YES',
                'CLANG_CXX_LIBRARY': 'libc++',
                'MACOSX_DEPLOYMENT_TARGET': '10.12'
            },
            'cflags!': [ '-fno-exceptions' ],
            'cflags_cc!': [ '-fno-exceptions' ],
            'msvs_settings': {
                'VCCLCompilerTool': { 'ExceptionHandling': 1 },
            }
        }
    ]
}