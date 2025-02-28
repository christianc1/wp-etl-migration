<?php
/**
 * WordPress User Extractor
 *
 * Extracts users from WordPress using Flow ETL.
 *
 * @package Flow\ETL\Adapter\WordPress
 */

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor\{Limitable, LimitableExtractor};
use Flow\ETL\{Extractor, FlowContext, Row, Rows};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Extractor\Signal;

final class WPUserExtractor implements Extractor, LimitableExtractor
{
    use Limitable;

    private array $userDefaults = [
        'role' => '',
        'role__in' => [],
        'role__not_in' => [],
        'include' => [],
        'exclude' => [],
        'orderby' => 'login',
        'order' => 'ASC',
        'number' => 100,
        'offset' => 0,
        'search' => '',
        'fields' => 'all_with_meta',
    ];

    private bool $includeMeta = true;
    private bool $includeCapabilities = false;

    public function __construct(
        private readonly array $config = []
    ) {
        $this->userDefaults = array_merge($this->userDefaults, $config['defaults'] ?? []);
        $this->resetLimit();
    }

    public function extract(FlowContext $context): \Generator
    {
        $offset = 0;
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        do {
            $args = array_merge($this->userDefaults, [
                'offset' => $offset,
            ]);

            $users = get_users($args);

            if (empty($users)) {
                break;
            }

            foreach ($users as $user) {
                $userData = $this->normalizeUser($user);

                if ($this->includeMeta) {
                    $userData['meta'] = get_user_meta($user->ID);
                }

                if ($this->includeCapabilities) {
                    $userData['capabilities'] = [
                        'roles' => $user->roles,
                        'allcaps' => $user->allcaps,
                        'caps' => $user->caps,
                    ];
                }

                if ($shouldPutInputIntoRows) {
                    $userData['_input_source'] = 'wp_users';
                }

                $signal = yield array_to_rows($userData, $context->entryFactory());
                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    return;
                }
            }

            $offset += $this->userDefaults['number'];
        } while (count($users) >= $this->userDefaults['number']);
    }

    private function normalizeUser(\WP_User $user): array
    {
        return [
            'ID' => (int) $user->ID,
            'user_login' => $user->user_login,
            'user_nicename' => $user->user_nicename,
            'user_email' => $user->user_email,
            'user_url' => $user->user_url,
            'user_registered' => $user->user_registered,
            'user_activation_key' => $user->user_activation_key,
            'user_status' => (int) $user->user_status,
            'display_name' => $user->display_name,
            'spam' => (bool) $user->spam,
            'deleted' => (bool) $user->deleted,
        ];
    }

    public function withRole(string $role): self
    {
        $clone = clone $this;
        $clone->userDefaults['role'] = $role;
        return $clone;
    }

    public function withRoleIn(array $roles): self
    {
        $clone = clone $this;
        $clone->userDefaults['role__in'] = $roles;
        return $clone;
    }

    public function withRoleNotIn(array $roles): self
    {
        $clone = clone $this;
        $clone->userDefaults['role__not_in'] = $roles;
        return $clone;
    }

    public function withInclude(array $userIds): self
    {
        $clone = clone $this;
        $clone->userDefaults['include'] = $userIds;
        return $clone;
    }

    public function withExclude(array $userIds): self
    {
        $clone = clone $this;
        $clone->userDefaults['exclude'] = $userIds;
        return $clone;
    }

    public function withOrderBy(string $orderBy): self
    {
        $clone = clone $this;
        $clone->userDefaults['orderby'] = $orderBy;
        return $clone;
    }

    public function withOrder(string $order): self
    {
        $clone = clone $this;
        $clone->userDefaults['order'] = strtoupper($order);
        return $clone;
    }

    public function withNumber(int $number): self
    {
        $clone = clone $this;
        $clone->userDefaults['number'] = $number;
        return $clone;
    }

    public function withSearch(string $search): self
    {
        $clone = clone $this;
        $clone->userDefaults['search'] = $search;
        return $clone;
    }

    public function withMeta(bool $includeMeta = true): self
    {
        $clone = clone $this;
        $clone->includeMeta = $includeMeta;
        return $clone;
    }

    public function withCapabilities(bool $includeCapabilities = true): self
    {
        $clone = clone $this;
        $clone->includeCapabilities = $includeCapabilities;
        return $clone;
    }
}
