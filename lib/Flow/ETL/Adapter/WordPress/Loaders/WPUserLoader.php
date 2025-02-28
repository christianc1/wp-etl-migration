<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress\Loaders;

use Flow\ETL\Adapter\WordPress\Exception\{
	WPAdapterDatabaseException,
	WPAdapterDataException,
	WPAdapterMissingDataException
};
use Flow\ETL\Adapter\WordPress\Normalizers\{
    EntryNormalizer,
    RowsNormalizer
};
use Flow\ETL\{FlowContext, Loader, Rows, Row};
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

    public function create_normalizer(FlowContext $context): RowsNormalizer
    {
        return new RowsNormalizer(new EntryNormalizer($context->config->caster(), $this->dateTimeFormat));
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            throw WPAdapterMissingDataException::noEntitiesFound('user', 'No users found to process');
        }

        $normalizer = $this->create_normalizer($context);

        foreach ($normalizer->normalize($rows) as $normalizedRow) {
            $this->insertUser($normalizedRow);
        }
    }

    public function insertUser(Row | array $row, RowsNormalizer | null $normalizer = null): int
    {
        // Normalize
        if ($row instanceof Row && $normalizer instanceof RowsNormalizer) {
            $data = $normalizer->normalize(new Rows([$row]))[0];
        } else {
            $data = $row;
        }

        // Sanitize input data
        $sanitizedData = $this->sanitizeUserData($data);

        // Required fields for user creation
        if (empty($sanitizedData['user.user_login']) && empty($sanitizedData['user.user_email'])) {
            throw WPAdapterDataException::missingRequiredData('user.user_login or user.user_email', $sanitizedData);
        }

        // Prepare user data
        $userData = array_merge($this->userDefaults, array_filter([
            'user_login' => $sanitizedData['user.user_login'] ?? '',
            'user_email' => $sanitizedData['user.user_email'] ?? '',
            'user_pass' => $sanitizedData['user.user_pass'] ?? wp_generate_password(),
            'user_nicename' => $sanitizedData['user.user_nicename'] ?? '',
            'user_url' => $sanitizedData['user.user_url'] ?? '',
            'display_name' => $sanitizedData['user.display_name'] ?? '',
            'nickname' => $sanitizedData['user.nickname'] ?? '',
            'first_name' => $sanitizedData['user.first_name'] ?? '',
            'last_name' => $sanitizedData['user.last_name'] ?? '',
            'description' => $sanitizedData['user.description'] ?? '',
            'role' => $sanitizedData['user.role'] ?? $this->userDefaults['role'],
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
            throw WPAdapterDatabaseException::fromWPError($userId, "Failed to insert/update user");
        }

        // Process meta fields
        foreach ($sanitizedData as $key => $value) {
            if (str_starts_with($key, 'meta.')) {
                $metaKey = sanitize_key(substr($key, 5)); // Remove 'meta.' prefix and sanitize

                // Sanitize meta value based on type
                $sanitizedValue = $this->sanitizeMetaValue($value);

                update_user_meta($userId, $metaKey, $sanitizedValue);
            }
        }

        // Handle roles if specified separately from the main role
        if (!empty($sanitizedData['user.roles']) && is_array($sanitizedData['user.roles'])) {
            $user = get_user_by('ID', $userId);
            if ($user) {
                $user->set_role(''); // Remove existing roles
                foreach ($sanitizedData['user.roles'] as $role) {
                    // Sanitize role
                    $role = sanitize_key($role);
                    if (get_role($role) !== null) {
                        $user->add_role($role);
                    }
                }
            }
        }

        return $userId;
    }

    /**
     * Sanitize user data before processing
     *
     * @param array $data Raw user data
     * @return array Sanitized user data
     */
    private function sanitizeUserData(array $data): array
    {
        $sanitized = [];

        // Sanitize user login
        if (isset($data['user.user_login'])) {
            $sanitized['user.user_login'] = sanitize_user($data['user.user_login']);
        }

        // Sanitize user email
        if (isset($data['user.user_email'])) {
            $sanitized['user.user_email'] = sanitize_email($data['user.user_email']);
        }

        // Sanitize user password (don't sanitize passwords as they may contain special characters)
        if (isset($data['user.user_pass'])) {
            $sanitized['user.user_pass'] = $data['user.user_pass'];
        }

        // Sanitize user nicename
        if (isset($data['user.user_nicename'])) {
            $sanitized['user.user_nicename'] = sanitize_title($data['user.user_nicename']);
        }

        // Sanitize user URL
        if (isset($data['user.user_url'])) {
            $sanitized['user.user_url'] = esc_url_raw($data['user.user_url']);
        }

        // Sanitize display name
        if (isset($data['user.display_name'])) {
            $sanitized['user.display_name'] = sanitize_text_field($data['user.display_name']);
        }

        // Sanitize nickname
        if (isset($data['user.nickname'])) {
            $sanitized['user.nickname'] = sanitize_text_field($data['user.nickname']);
        }

        // Sanitize first name
        if (isset($data['user.first_name'])) {
            $sanitized['user.first_name'] = sanitize_text_field($data['user.first_name']);
        }

        // Sanitize last name
        if (isset($data['user.last_name'])) {
            $sanitized['user.last_name'] = sanitize_text_field($data['user.last_name']);
        }

        // Sanitize description (can contain limited HTML)
        if (isset($data['user.description'])) {
            $sanitized['user.description'] = wp_kses_post($data['user.description']);
        }

        // Sanitize role
        if (isset($data['user.role'])) {
            $role = sanitize_key($data['user.role']);
            $sanitized['user.role'] = get_role($role) !== null ? $role : 'subscriber';
        }

        // Sanitize roles array
        if (isset($data['user.roles']) && is_array($data['user.roles'])) {
            $sanitized['user.roles'] = array_map('sanitize_key', $data['user.roles']);
        }

        // Pass through meta fields for later processing
        foreach ($data as $key => $value) {
            if (str_starts_with($key, 'meta.')) {
                $sanitized[$key] = $value; // We'll sanitize the meta values later
            } elseif (!isset($sanitized[$key])) {
                // Pass through any other fields that weren't explicitly sanitized
                $sanitized[$key] = $value;
            }
        }

        return $sanitized;
    }

    /**
     * Sanitize a meta value based on its type
     *
     * @param mixed $value The meta value to sanitize
     * @return mixed Sanitized meta value
     */
    private function sanitizeMetaValue($value)
    {
        if (is_numeric($value)) {
            // If it's a numeric value, preserve its type (int or float)
            return is_float($value + 0) ? (float)$value : (int)$value;
        } elseif (is_string($value)) {
            // For strings, use appropriate sanitization
            if (strpos($value, '<') !== false && strpos($value, '>') !== false) {
                // If it looks like HTML, use wp_kses_post
                return wp_kses_post($value);
            } else {
                // Otherwise use standard text sanitization
                return sanitize_text_field($value);
            }
        } elseif (is_array($value)) {
            // For arrays, recursively sanitize each element
            return array_map([$this, 'sanitizeMetaValue'], $value);
        } elseif (is_bool($value)) {
            // Preserve boolean values
            return $value;
        } elseif (is_null($value)) {
            // Preserve null values
            return $value;
        } else {
            // For any other type, return as is
            return $value;
        }
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
