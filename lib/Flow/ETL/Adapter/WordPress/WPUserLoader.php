<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress;

use Flow\ETL\Adapter\WordPress\RowsNormalizer\EntryNormalizer;
use Flow\ETL\{FlowContext, Loader, Rows};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;

final class WPUserLoader implements Loader
{
    private string $dateTimeFormat = \DateTimeInterface::ATOM;
    private array $userDefaults = [
        'role' => 'subscriber',
    ];

    public function __construct(
        private readonly array $config = []
    ) {
        $this->userDefaults = array_merge($this->userDefaults, $config['defaults'] ?? []);
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            return;
        }

        $normalizer = new RowsNormalizer(new EntryNormalizer($context->config->caster(), $this->dateTimeFormat));

        foreach ($normalizer->normalize($rows) as $normalizedRow) {
            $this->insertUser($normalizedRow);
        }
    }

    private function insertUser(array $data): int
    {
        // Required fields for user creation
        if (empty($data['user.user_login']) && empty($data['user.user_email'])) {
            throw new RuntimeException('Either user_login or user_email is required');
        }

        // Prepare user data
        $userData = array_merge($this->userDefaults, array_filter([
            'user_login' => $data['user.user_login'] ?? '',
            'user_email' => $data['user.user_email'] ?? '',
            'user_pass' => $data['user.user_pass'] ?? wp_generate_password(),
            'user_nicename' => $data['user.user_nicename'] ?? '',
            'user_url' => $data['user.user_url'] ?? '',
            'display_name' => $data['user.display_name'] ?? '',
            'nickname' => $data['user.nickname'] ?? '',
            'first_name' => $data['user.first_name'] ?? '',
            'last_name' => $data['user.last_name'] ?? '',
            'description' => $data['user.description'] ?? '',
            'role' => $data['user.role'] ?? $this->userDefaults['role'],
        ]));

        // Check if user exists
        $existingUser = null;
        if (!empty($userData['user_email'])) {
            $existingUser = get_user_by('email', $userData['user_email']);
        }
        if (!$existingUser && !empty($userData['user_login'])) {
            $existingUser = get_user_by('login', $userData['user_login']);
        }

        // Insert or update user
        if ($existingUser) {
            $userData['ID'] = $existingUser->ID;
            $userId = wp_update_user($userData);
        } else {
            $userId = wp_insert_user($userData);
        }

        if (is_wp_error($userId)) {
            throw new RuntimeException("Failed to insert/update user: " . $userId->get_error_message());
        }

        // Process meta fields
        foreach ($data as $key => $value) {
            if (str_starts_with($key, 'meta.')) {
                $metaKey = substr($key, 5); // Remove 'meta.' prefix
                update_user_meta($userId, $metaKey, $value);
            }
        }

        // Handle roles if specified separately from the main role
        if (!empty($data['user.roles']) && is_array($data['user.roles'])) {
            $user = get_user_by('ID', $userId);
            if ($user) {
                $user->set_role(''); // Remove existing roles
                foreach ($data['user.roles'] as $role) {
                    $user->add_role($role);
                }
            }
        }

        return $userId;
    }

    public function withDateTimeFormat(string $dateTimeFormat): self
    {
        $clone = clone $this;
        $clone->dateTimeFormat = $dateTimeFormat;
        return $clone;
    }

    public function withDefaults(array $defaults): self
    {
        $clone = clone $this;
        $clone->userDefaults = array_merge($this->userDefaults, $defaults);
        return $clone;
    }
}
