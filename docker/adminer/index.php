<?php
namespace docker {
	function adminer_object() {
		/**
		 * Prefills the “Server” field with the ADMINER_DEFAULT_SERVER environment variable.
		 */
		final class DefaultServerPlugin extends \Adminer\Plugin {
			public function __construct(
				private \Adminer\Adminer $adminer
			) { }

			public function loginFormField(...$args): string {
				return (function (...$args): string {
					$field = $this->loginFormField(...$args);

					return \preg_replace_callback(
						'/name="auth\[server\]" value="" title="(?:[^"]+)"/',
						static function (array $matches): string {
							$defaultServer = $_ENV['ADMINER_DEFAULT_SERVER'] ?: ($_ENV['POSTGRES_HOST'] ?: 'db');

							return \str_replace(
								'value=""',
								\sprintf('value="%s"', $defaultServer),
								$matches[0],
							);
						},
						$field,
					);
				})->call($this->adminer, ...$args);
			}
		}

		$plugins = [];
		foreach (glob('plugins-enabled/*.php') as $plugin) {
			$plugins[] = require($plugin);
		}

		$adminer = new \Adminer\Plugins($plugins);

		(function () {
			$last = &$this->hooks['loginFormField'][\array_key_last($this->hooks['loginFormField'])];
			if ($last instanceof \Adminer\Adminer) {
				$defaultServerPlugin = new DefaultServerPlugin($last);
				$this->plugins[] = $defaultServerPlugin;
				$last = $defaultServerPlugin;
			}
		})->call($adminer);

		return $adminer;
	}
}

namespace {
	if (basename($_SERVER['DOCUMENT_URI'] ?? $_SERVER['REQUEST_URI']) === 'adminer.css' && is_readable('adminer.css')) {
		header('Content-Type: text/css');
		readfile('adminer.css');
		exit;
	}

	function adminer_object() {
		return \docker\adminer_object();
	}

	function adminer_autologin_enabled(): bool {
		if (!filter_var($_ENV['ADMINER_AUTOLOGIN'] ?? false, FILTER_VALIDATE_BOOLEAN)) {
			return false;
		}

		if (!empty($_POST) || isset($_GET['manual'])) {
			return false;
		}

		if (!empty($_GET['username'])) {
			return false;
		}

		foreach (['POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD'] as $requiredEnv) {
			if (($_ENV[$requiredEnv] ?? '') === '') {
				return false;
			}
		}

		return true;
	}

	if (adminer_autologin_enabled()) {
		$_POST['auth'] = [
			'driver' => 'pgsql',
			'server' => $_ENV['ADMINER_DEFAULT_SERVER'] ?: ($_ENV['POSTGRES_HOST'] ?: 'db'),
			'username' => $_ENV['POSTGRES_USER'],
			'password' => $_ENV['POSTGRES_PASSWORD'],
			'db' => $_ENV['POSTGRES_DB'],
		];
	}

	require('adminer.php');
}
