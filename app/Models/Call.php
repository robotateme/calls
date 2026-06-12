<?php

declare(strict_types=1);

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

/**
 * @property int $id
 * @property string $external_call_id
 * @property string $phone
 * @property string $kafka_message_id
 * @property string $status
 * @property int|null $client_id
 * @property int|null $operator_id
 * @property int $operator_search_attempts
 * @property int $operator_search_max_attempts
 * @property int $operator_search_retry_delay_seconds
 * @property string $operator_search_hangup_policy
 * @property string|null $next_operator_search_at
 * @property string|null $assignment_requested_at
 * @property string|null $operator_ringing_at
 * @property string|null $connected_at
 */
final class Call extends Model
{
    protected $fillable = [
        'external_call_id',
        'phone',
        'kafka_message_id',
        'status',
        'client_id',
        'operator_id',
        'operator_search_attempts',
        'operator_search_max_attempts',
        'operator_search_retry_delay_seconds',
        'operator_search_hangup_policy',
        'next_operator_search_at',
        'assignment_requested_at',
        'operator_ringing_at',
        'connected_at',
    ];

    /**
     * @return BelongsTo<Client, $this>
     */
    public function client(): BelongsTo
    {
        return $this->belongsTo(Client::class);
    }

    /**
     * @return BelongsTo<Operator, $this>
     */
    public function operator(): BelongsTo
    {
        return $this->belongsTo(Operator::class);
    }
}
